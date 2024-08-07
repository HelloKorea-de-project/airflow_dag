import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.dagbag import DagBag
from airflow.utils.dates import days_ago
from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig, DbtTaskGroup
from cosmos.constants import LoadMode
from croniter import croniter

from plugins import slack

logger = logging.getLogger(__name__)

# Constants for DBT configuration
DBT_PROJECT_NAME = "helloword_dbt"
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

DAG_EXECUTE_HOUR = 11

profile_config = ProfileConfig(
    profile_name=DBT_PROJECT_NAME,
    target_name="prod",
    profiles_yml_filepath=DBT_ROOT_PATH / f"{DBT_PROJECT_NAME}/profiles.yml"
)

def extract_dependent_dags():
    """
    Extract model names and the last task ID from DAG descriptions under the 'load_target_models' key.
    Returns a list of tuples (dag_id, model_name, last_task_id, execution_delta).
    """
    logger.info("Extracting dependent DAGs...")

    dag_bag = DagBag(include_examples=False)
    models = []

    # Regular expression to match the dependent_models pattern
    pattern = re.compile(r"load_target_models\s*:\s*\[\s*(.*?)\s*\]")

    for dag_id, dag in dag_bag.dags.items():
        if ('load-redshift' in dag.tags) and dag.description:
            logger.debug(f"Processing DAG: {dag_id}")

            # Search for the pattern in the DAG's description
            match = pattern.search(dag.description)
            logger.debug(f"Match found in DAG {dag_id}: {match.group(0)}")
            if match:
                # Extract and clean the model names
                model_names = [name.strip().strip("'\"") for name in match.group(1).split(',') if name.strip()]

                # Get the last task ID
                if dag.tasks:
                    last_task_id = dag.tasks[-1].task_id
                else:
                    continue

                # Calculate execution_delta based on schedule_interval
                if dag.schedule_interval is not None:
                    execution_delta = calculate_execution_delta(dag.schedule_interval)
                else:
                    execution_delta = timedelta(days=1)  # Default to 1 day if schedule_interval is not set

                for model_name in model_names:
                    models.append((dag_id, model_name.strip(), last_task_id, execution_delta))

    if not models:
        raise ValueError("No models found in DAG descriptions.")

    return models


def calculate_execution_delta(schedule_interval):
    """
    Calculate the execution_delta based on the upstream DAG's schedule_interval.
    """
    if isinstance(schedule_interval, timedelta):
        return schedule_interval

    # If schedule_interval is a cron expression, calculate the time difference
    try:
        iter = croniter(schedule_interval, datetime.now())
        next_run_time = iter.get_next(datetime)
        current_time = datetime.now()

        # Assume downstream runs daily at DAG_EXECUTE_HOUR
        downstream_schedule_time = current_time.replace(hour=DAG_EXECUTE_HOUR, minute=0, second=0, microsecond=0)
        if downstream_schedule_time < current_time:
            # If the downstream DAG time has passed today, consider it for the next day
            downstream_schedule_time += timedelta(days=1)

        execution_delta = downstream_schedule_time - next_run_time

        # Ensure the delta is positive and represents a valid time period
        if execution_delta.total_seconds() < 0:
            execution_delta += timedelta(days=1)

        logger.debug(f"Calculated execution_delta: {execution_delta}")
        return execution_delta
    except Exception as e:
        logger.error(f"Error calculating execution_delta for schedule_interval '{schedule_interval}': {str(e)}")
        return timedelta(days=1)  # Default if something goes wrong

def create_dynamic_dag(dag_id, external_dag_id, external_task_id, model_name, execution_delta):
    """
    Create a dynamic DAG with an ExternalTaskSensor and a DbtDag task.
    """
    default_args = {
        'owner': 'dbt',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 7),
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=f'0 {DAG_EXECUTE_HOUR} * * *',
        catchup=False,
    ) as dag:

        # ExternalTaskSensor for each model
        wait_for_external_task = ExternalTaskSensor(
            task_id='sensing_upstream_model_task',
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            poke_interval=1800,
            execution_delta=execution_delta,
            timeout=14400,
            mode='reschedule',
            check_existence=True
        )

        # DbtDag for each model
        dbt_dag = DbtTaskGroup(
            group_id=f"dbt_test_task_{model_name}",
            project_config=ProjectConfig(
                DBT_ROOT_PATH / DBT_PROJECT_NAME,
            ),
            profile_config=profile_config,
            operator_args={
                "install_deps": True,
                # "dbt_cmd_flags": ["--models", "stg_customers"],
            },
            render_config=RenderConfig(
                select=[model_name],
                load_method=LoadMode.DBT_LS,
            ),
            default_args={"retries": 1},
            on_warning_callback=slack.warning_data_quality_callback,
        )

        wait_for_external_task >> dbt_dag

    return dag

# Create dynamic DAGs based on the extracted information
model_info = extract_dependent_dags()
for external_dag_id, model_name, external_task_id, execution_delta in model_info:
    dag_id = f"dynamic_test_{model_name}"
    globals()[dag_id] = create_dynamic_dag(dag_id, external_dag_id, external_task_id, model_name, execution_delta)
