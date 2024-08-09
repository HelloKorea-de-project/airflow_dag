from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig, DbtTaskGroup
from cosmos.constants import LoadMode
from pathlib import Path
import os
import slack

# Constants for DBT configuration
DBT_PROJECT_NAME = "hellokorea_dbt"
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name=DBT_PROJECT_NAME,
    target_name="prod",
    profiles_yml_filepath=DBT_ROOT_PATH / f"{DBT_PROJECT_NAME}/profiles.yml"
)

def create_dbt_task_group(group_id, select_models, dag):
    """
    Helper function to create a DbtTaskGroup with common configurations.
    """
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            DBT_ROOT_PATH / DBT_PROJECT_NAME,
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
        },
        render_config=RenderConfig(
            select=select_models,
            load_method=LoadMode.DBT_LS,
        ),
        default_args={"retries": 1},
        on_warning_callback=slack.warning_data_quality_callback,
        dag=dag,
    )