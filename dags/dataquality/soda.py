import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

soda_path = "/opt/airflow/include/soda"
datasource = "pg_datasource"

def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command=f"soda scan -d {datasource} -c {soda_path}/configuration.yaml -v SCHEMA={schema} {soda_path}/checks.yaml"
        )
        return task
    except Exception as e:
        logger.error(f"Error in yt_elt_data_quality: {schema}")
        raise e