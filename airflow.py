from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='spark_java_jobs_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run ICEGERGERT data pipeline',
) as dag:

    start = DummyOperator(task_id='start')


    # flink_job = BashOperator(
    #     task_id='flink_job',
    #     bash_command='flink run -c com.example.FlinkStageJob /opt/flink-jobs/flink-stage-job.jar',
    # )

    spark_batch_job_postgres = SparkSubmitOperator(
        task_id='spark_batch_job_1',
        application='/opt/spark-jobs/batch-job-1.jar',
        conn_id='spark_default',
        java_class='com.example.BatchJob1',
    )

    spark_batch_job_iceberg = SparkSubmitOperator(
        task_id='spark_batch_job_2',
        application='/opt/spark-jobs/batch-job-2.jar',
        conn_id='spark_default',
        java_class='com.example.BatchJob2',
    )

    spark_dwh_job = SparkSubmitOperator(
        task_id='spark_dwh_job',
        application='/opt/spark-jobs/dwh-job.jar',
        conn_id='spark_default',
        java_class='com.example.DWHJob',
    )

    spark_final_job = SparkSubmitOperator(
        task_id='spark_final_job',
        application='/opt/spark-jobs/final-job.jar',
        conn_id='spark_default',
        java_class='com.example.FinalJob',
    )

    end = DummyOperator(task_id='end')

    
    start >> [spark_batch_job_postgres, spark_batch_job_iceberg]
    [spark_batch_job_postgres, spark_batch_job_iceberg] >> spark_dwh_job >> spark_final_job >> end