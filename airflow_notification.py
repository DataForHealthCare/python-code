import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.utils.timezone import datetime
from astro_databricks.operators.notebook import DatabricksNotebookOperator
from astro_databricks.operators.workflow import DatabricksWorkflowTaskGroup
from adwhicore.shared_modules.notification_utils import teams_alert


class AirFlowNotification:
    """
    Container class for the Microsoft Teams notification: 
        - Microsoft Teams message body content
        - A static method for sending a Microsoft Teams notification alert
        - Shared module 'notification_utils' utilizing the 'teams_alert' function
    ...

    Class Attributes
    ----------
    teams_title : str
        main identifying notification title
    teams_subtitle : str
        notification subtitle
    teams_webhook_url : str
        configured webhook url for the specified MS Teams channel

    Methods
    -------
    ms_teams_alert(context):
        - Sets message body content based on the AirFLow context dictionary & Databricks notebook path
        - Passes arguments into the 'teams_alert' shared module
        - Sends the notification to the specified MS Teams channel
    """

    teams_title = "AirFlow Workflow"
    teams_subtitle = "Failure Notification"
    teams_webhook_url = "https://healthpartnersconnect.webhook.office.com/" \
        + "webhookb2/8e6d2cfc-f3ff-4678-ab5e-f98806ce95ef@9539230a-5213-4542-9ca6-b0ec58c41a4d/" \
            + "IncomingWebhook/fbf218f68dc4453f9c4885bdccb05a6b/1775bd76-474f-4197-be8c-9e2dfd57b872"

    @staticmethod
    def ms_teams_alert(context):
        teams_message = [
            {"name": "Dag Id", "value": "{}".format(context["ti"].dag_id)},
            {"name": "Task Id", "value": "{}".format(context["ti"].task_id)},
            {"name": "State", "value": "{}".format(context["ti"].state)},
            {"name": "Run Id", "value": "{}".format(context["ti"].run_id)},
            {"name": "Start Date", "value": "{}".format(context["ti"].start_date)},
            {"name": "End Date", "value": "{}".format(context["ti"].end_date)},
            {
                "name": "Databricks Notebook Path",
                "value": "/Repos/j7259@healthpartners.com/Data-Ingestion-Pipeline/" \
                    + "Databricks/ProductTeam/HICORE/product/test",
            },
        ]
        teams_alert(
            AirFlowNotification.teams_title,
            AirFlowNotification.teams_subtitle,
            teams_message,
            AirFlowNotification.teams_webhook_url,
        )


class Parameters:
    """
    Container class for parameters relating to Databricks Operators:
        - Specifications for a new Databricks cluster
        - A function to configure the DAG start date 'timezone'
    ...

    Class Attributes
    ----------
    new_cluster : list
        'DatabricksWorkflowTaskGroup' parameters for a new cluster
    local_tz : function
        function to configure the timezone
    """

    new_cluster = [
        {
            "job_cluster_key": "Job_Cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "13.3.x-scala2.12",
                "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
                "node_type_id": "Standard_DS3_v2",
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                "autoscale": {"min_workers": 1, "max_workers": 6},
            },
        }
    ]
    local_tz = pendulum.timezone("America/Chicago")


class DefaultArgs:
    """
    Class container of default arguments intended to optimize DAG configurations and streamline workflow management:
        - A dictionary of default parameters used by all tasks in the DAG
    ...

    Class Attributes
    ----------
    default_args : dictionary
        default parameters for all DAG tasks
    """

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_success": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }

dag = DAG(
    dag_id="ms_teams_test_adwhicore_template",
    start_date=datetime(2024, 1, 31, tzinfo=Parameters.local_tz),
    schedule_interval="30 7 * * *",
    catchup=False,
    default_args=DefaultArgs.default_args,
    tags=["standardized", "ADWHICORE"],
    is_paused_upon_creation=False,
)

with dag:
    """
    A directed acyclic graph is an organized collection of dependent tasks. The DAG dictates how relational tasks run. 
    ...

    DAG Operators
    ----------
    DatabricksWorkflowTaskGroup : function
        creates a Databricks workflow for notebooks specified in the task group
    DatabricksNotebookOperator : function
        runs a Databricks notebook

    AirFlow Callbacks
    ----------
    on_failure_callback : function
        executes the teams_alert shared module on task failure
    """
    task_group = DatabricksWorkflowTaskGroup(
        group_id="ms_teams_test_adwhicore",
        databricks_conn_id="Databricks",
        job_clusters=Parameters.new_cluster,
    )
    with task_group:
        adwhicore_test = DatabricksNotebookOperator(
            task_id="ms_teams_test_adwhicore_group_1",
            databricks_conn_id="Databricks",
            notebook_path="/Repos/j7259@healthpartners.com/Data-Ingestion-Pipeline/Databricks/ProductTeam/HICORE/product/test",
            source="WORKSPACE",
            job_cluster_key="Job_Cluster",
            notebook_params={"source_environment": "prd", "target_environment": "dev"},
            on_failure_callback=AirFlowNotification.ms_teams_alert,
        )
        adwhicore_test