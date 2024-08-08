import logging
import os
import re
import json
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
DBT_PROJECT_NAME = "hellokorea_dbt"
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

DAG_EXECUTE_HOUR = 11

profile_config = ProfileConfig(
    profile_name=DBT_PROJECT_NAME,
    target_name="prod",
    profiles_yml_filepath=DBT_ROOT_PATH / f"{DBT_PROJECT_NAME}/profiles.yml"
)


def get_dbt_resources_and_lineage(project_dir):
    '''
    Function to process the dbt ls output and extract model and source information
    '''
    
    # Path to the manifest.json file which contains dbt resource information
    manifest_path = os.path.join(project_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.debug(f"Manifest file not found at {manifest_path}")
        raise ValueError(f"Manifest file not found at {manifest_path}")

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    # Extract models
    models = {
        node['name']: {
            'name': node['name'],
            'description': node.get('description', ''),
            'columns': node.get('columns', {}),
            'depends_on': node['depends_on']['nodes']
        }
        for node in manifest['nodes'].values()
        if node['resource_type'] == 'model'
    }

    # Extract sources and create a mapping of source alias to identifier
    sources = {
        source['identifier']: {
            'unique_id': source['unique_id'],
            'database': source['database'],
            'schema': source['schema'],
            'name': source['name'],
            'resource_type': source['resource_type'],
            'package_name': source['package_name'],
            'source_name': source['source_name'],
            'identifier': source['identifier'],
        }
        for source in manifest['sources'].values()
    }

    return models, sources


def get_model_by_source_identifier(identifier, models, sources):
    '''
    Function to check if a source identifier is registered in dbt and get the corresponding model information
    '''

    source = sources.get(identifier)
    if not source:
        return None, f"Source identifier '{identifier}' is not registered."

    # Find the model whose depends_on contains the source identifier
    matching_models = [
        model for model in models.values()
        if any(
            source['unique_id'] in dependency
            for dependency in model['depends_on']
        )
    ]
    if not matching_models:
        return None, f"No model found for source identifier '{identifier}' with source name '{source['name']}'."

    # Return the first matching model (assuming there is only one match)
    return matching_models[0], None

def extract_dependent_dags():
    """
    Extract model names and the last task ID from DAG descriptions under the 'load_target_models' key.
    Returns a list of tuples (dag_id, model_name, last_task_id, execution_delta).
    """
    logger.info("Extracting dependent DAGs...")

    dag_bag = DagBag(include_examples=False)
    
    dag_models = []
    available_models = []

    models, sources = get_dbt_resources_and_lineage(DBT_ROOT_PATH / DBT_PROJECT_NAME)
    if not (models and sources):
        logger.error("No models found in DBT.")
        raise ValueError(f"No models found in DBT : {len(models)}, {len(sources)}")

    for dag_id, dag in dag_bag.dags.items():
        if 'load-redshift' in dag.tags:
            # Process the DAG if it has the 'load-redshift' tag
            for tag in dag.tags:
                if tag.startswith('table:'):
                    # Extract the table name from the tag and convert it to lowercase
                    table_name = tag.split(':')[1].lower()

                    model_info, error = get_model_by_source_identifier(table_name, models, sources)
                    if error:
                        logger.debug(error)
                    else:
                        dag_models.append(model_info)
                        logger.debug(f"Model Information for Source Identifier '{table_name}': {model_info}")

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

            for model in dag_models:
                available_models.append((dag_id, model['name'].strip(), last_task_id, execution_delta))

    if not available_models:
        raise ValueError(f"No models found in DAG tags : {len(available_models)}")

    return available_models


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
            poke_interval=5 * 60,
            execution_delta=execution_delta,
            timeout=30 * 60,
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
if model_info:
    for external_dag_id, model_name, external_task_id, execution_delta in model_info:
        dag_id = f"dynamic_test_{model_name}"
        globals()[dag_id] = create_dynamic_dag(dag_id, external_dag_id, external_task_id, model_name, execution_delta)
else:
    raise ValueError(f"No model_info founded : {len(model_info)}")