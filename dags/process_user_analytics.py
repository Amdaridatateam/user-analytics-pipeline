import google
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator


GCP_CONN_ID = "gcp_airflow_conn"
GCS_BUCKET_NAME = "useranalytics-pipeline-bucket"
LOG_REVIEW_KEY_NAME = "data/log_reviews.csv"
MOVIE_REVIEW_KEY_NAME = "data/movie_review.csv"


POSTGRES_CONN_ID = "postgres_db"
SCHEMA_NAME = 'ecommerce'
TABLE_NAME = "user_purchase"
GCP_PROJECT_ID = 'user-behaviour-project'
CLUSTER_NAME = 'dataproc-cluster'


MOVIE_REVIEW_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/process_movie_review.py'
LOG_REVIEW_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/process_log_review.py'
LOAD_TO_BIGQUERY_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/load_data_to_bq.py'


def form_pyspark_job(job_file_uri):
    PYSPARK_JOB = dict(
        reference=dict(project_id=GCP_PROJECT_ID),
        placement=dict(cluster_name=CLUSTER_NAME),
        pyspark_job=dict(main_python_file_uri=job_file_uri)
    )

    return PYSPARK_JOB


def _create_or_use_cluster(cluster_name, project_id, region):
    dataproc_hook = DataprocHook(gcp_conn_id=GCP_CONN_ID)
    try:
        cluster_info = dataproc_hook.get_cluster(
            cluster_name=cluster_name,
            project_id=project_id,
            region=region
        )
        print(cluster_info)
        return "continue_pipeline"

    except google.api_core.exceptions.NotFound:
        return "create_dataproc_cluster"

DATAPROC_CLUSTER_CONFIG = ClusterGenerator(
    project_id=GCP_PROJECT_ID,
    zone='us-central1-a',
    master_machine_type='n2-standard-2',
    master_disk_size=32,
    worker_machine_type='n2-standard-2',
    worker_disk_size=32,
    num_workers=2,
    idle_delete_ttl=1200,
    metadata=dict(
        PIP_PACKAGES="spacy==3.2.1 regex==2023.10.3 nltk"
    )
).make()

with DAG(
    "process_user_analytics",
    start_date=days_ago(1),
    schedule="@once",
    description="A DAG to create spark jobs",
    catchup=False,
) as dag:
    
    start_workflow = EmptyOperator(task_id="start_workflow")

    verify_today_movie_review = GCSObjectExistenceSensor(
        task_id="verify_today_movie_review",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=MOVIE_REVIEW_KEY_NAME,
    )

    verify_today_log_review = GCSObjectExistenceSensor(
        task_id="verify_today_log_review",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=LOG_REVIEW_KEY_NAME,
    )

    check_dataproc_cluster_exists = BranchPythonOperator(
        task_id='check_dataproc_cluster_exists',
        python_callable=_create_or_use_cluster,
        op_kwargs=dict(
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT_ID,
            region='us-central1'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    continue_pipeline = EmptyOperator(task_id="continue_pipeline")

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        use_if_exists=True,
        region='us-central1',
        project_id=GCP_PROJECT_ID,
    )

    continue_workflow = EmptyOperator(task_id='continue_workflow', trigger_rule=TriggerRule.ONE_SUCCESS)


    submit_log_review_job = DataprocSubmitJobOperator(
        task_id="submit_log_review_job",
        job=form_pyspark_job(LOG_REVIEW_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
    )

    submit_movie_review_job = DataprocSubmitJobOperator(
        task_id="submit_movie_review_job",
        job=form_pyspark_job(MOVIE_REVIEW_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
    )

    submit_load_to_bigquery_job = DataprocSubmitJobOperator(
        task_id="submit_load_to_bigquery_job",
        job=form_pyspark_job(LOAD_TO_BIGQUERY_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    end_workflow = EmptyOperator(task_id='end_workflow', trigger_rule=TriggerRule.ALL_SUCCESS)

    (
        start_workflow 
        >> [verify_today_log_review , verify_today_movie_review ] 
        >> check_dataproc_cluster_exists 
        >> [create_dataproc_cluster, continue_pipeline]
        >> continue_workflow
        >> [submit_log_review_job, submit_movie_review_job] 
        >> submit_load_to_bigquery_job
        >> end_workflow
    )