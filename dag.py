from datetime import datetime
from importlib.resources import path

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.task_group import TaskGroup

from pyspark.sql.types import *

from airflow.models import Variable


POSTER_LIMIT = 10


with DAG(
        dag_id="final_project_dag_id",
        schedule_interval=None,
        start_date=datetime(year=2022, day=1, month=2),
        catchup=False,
        tags=['FreeUni']
) as dag:
        install_imdbpy_command = (
        """
                pip install imdbpy &
                pip install urllib3 & 
                pip install matplotlib & 
                pip install seaborn &
                pip install keras &
                pip install --upgrade tensorflow &
                pip install numpy &
                apt-get update && apt-get install -y python3-opencv &
                pip install opencv-python 
        """
        )
        
        imdbpy_bash = BashOperator(task_id="install_imdbpy_id", 
                                bash_command=install_imdbpy_command, 
                                depends_on_past=False)

        path='/airflow/jobs/'
        cast_submit = SparkSubmitOperator(application=f'{path}cast_job.py',
                                                task_id="cast_submit_id")

        movies_meta_submit = SparkSubmitOperator(application=f'{path}movies_meta_job.py',
                                                task_id="movies_meta_submit_id")

        with TaskGroup("save_posters_group") as save_posters_group:
 
            num_threads = int(Variable.get("num_threads"))

            for i in range(num_threads):
                SparkSubmitOperator(application=f'{path}download_posters_job.py',
                                                task_id=f"download_posters_id{i}",
                                                application_args=[str(i), str(num_threads)])

            

        model_submit = SparkSubmitOperator(application=f'{path}create_model_job.py',
                                                task_id=f"poster_classification_id",)
        
        imdbpy_bash >> [cast_submit, movies_meta_submit] >> save_posters_group >> model_submit