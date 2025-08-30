from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator

# Custom SSHOperator class to disable templating
class NoTemplateSSHOperator(SSHOperator):
    template_fields = []

# Common default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

SSH_CONN_ID = 'controlplane_ssh'

# =============================================================================
# 1. CSV PIPELINE - Runs at 00:00 daily
# =============================================================================
csv_dag = DAG(
    'csv_delta_table_pipeline',
    default_args=default_args,
    description='CSV to Delta Table processing pipeline: Kafka-MinIO -> Bronze -> Silver -> Gold',
    schedule_interval='0 0 * * *',  # Daily at 00:00
    tags=['delta-lake', 'csv', 'etl', 'kafka-minio'],
    max_active_runs=1
)

csv_start = DummyOperator(task_id='start_pipeline', dag=csv_dag)

csv_kafka_minio = NoTemplateSSHOperator(
    task_id='kafka_minio_csv_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/kafka_minio_Multiple_pipes && python3 run_kafka_minio_transaction.py',
    cmd_timeout=1800,
    dag=csv_dag
)

csv_bronze = NoTemplateSSHOperator(
    task_id='bronze_csv_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/bronze && chmod +x run_csv_to_deltatable.sh && ./run_csv_to_deltatable.sh',
    cmd_timeout=1800,
    dag=csv_dag
)

csv_silver = NoTemplateSSHOperator(
    task_id='silver_csv_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/silver && chmod +x run_csv_to_deltatable.sh && ./run_csv_to_deltatable.sh',
    cmd_timeout=1800,
    dag=csv_dag
)

csv_gold = NoTemplateSSHOperator(
    task_id='gold_csv_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/gold && chmod +x run_csv_to_deltatable.sh && ./run_csv_to_deltatable.sh',
    cmd_timeout=1800,
    dag=csv_dag
)

csv_end = DummyOperator(task_id='pipeline_complete', dag=csv_dag)
csv_start >> csv_kafka_minio >> csv_bronze >> csv_silver >> csv_gold >> csv_end

# =============================================================================
# 2. IMAGE PIPELINE - Runs at 02:00 daily (2 hours after CSV)
# =============================================================================
image_dag = DAG(
    'image_delta_table_pipeline',
    default_args=default_args,
    description='Image to Delta Table processing pipeline: Kafka-MinIO -> Bronze -> Silver -> Gold',
    schedule_interval='0 2 * * *',  # Daily at 02:00
    tags=['delta-lake', 'image', 'etl', 'kafka-minio'],
    max_active_runs=1
)

image_start = DummyOperator(task_id='start_pipeline', dag=image_dag)

image_kafka_minio = NoTemplateSSHOperator(
    task_id='kafka_minio_image_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/kafka_minio_Multiple_pipes && chmod +x run_kafka_minio_image.sh && ./run_kafka_minio_image.sh',
    cmd_timeout=1800,
    dag=image_dag
)

image_bronze = NoTemplateSSHOperator(
    task_id='bronze_image_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/bronze && chmod +x run_image_to_deltatable.sh && ./run_image_to_deltatable.sh',
    cmd_timeout=1800,
    dag=image_dag
)

image_silver = NoTemplateSSHOperator(
    task_id='silver_image_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/silver && chmod +x run_image_to_deltatable.sh && ./run_image_to_deltatable.sh',
    cmd_timeout=1800,
    dag=image_dag
)

image_gold = NoTemplateSSHOperator(
    task_id='gold_image_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/gold && chmod +x run_image_to_deltatable.sh && ./run_image_to_deltatable.sh',
    cmd_timeout=1800,
    dag=image_dag
)

image_end = DummyOperator(task_id='pipeline_complete', dag=image_dag)
image_start >> image_kafka_minio >> image_bronze >> image_silver >> image_gold >> image_end

# =============================================================================
# 3. SENSOR PIPELINE - Runs at 04:00 daily (2 hours after Image)
# =============================================================================
sensor_dag = DAG(
    'sensor_delta_table_pipeline',
    default_args=default_args,
    description='Sensor to Delta Table processing pipeline: Kafka-MinIO -> Bronze -> Silver -> Gold',
    schedule_interval='0 4 * * *',  # Daily at 04:00
    tags=['delta-lake', 'sensor', 'etl', 'kafka-minio'],
    max_active_runs=1
)

sensor_start = DummyOperator(task_id='start_pipeline', dag=sensor_dag)

sensor_kafka_minio = NoTemplateSSHOperator(
    task_id='kafka_minio_sensor_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/kafka_minio_Multiple_pipes && python3 run_kafka_minio_sensor.py',
    cmd_timeout=1800,
    dag=sensor_dag
)

sensor_bronze = NoTemplateSSHOperator(
    task_id='bronze_sensor_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/bronze && chmod +x run_sensor_to_deltatable.sh && ./run_sensor_to_deltatable.sh',
    cmd_timeout=1800,
    dag=sensor_dag
)

sensor_silver = NoTemplateSSHOperator(
    task_id='silver_sensor_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/silver && chmod +x run_sensor_to_deltatable.sh && ./run_sensor_to_deltatable.sh',
    cmd_timeout=1800,
    dag=sensor_dag
)

sensor_gold = NoTemplateSSHOperator(
    task_id='gold_sensor_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/gold && chmod +x run_sensor_to_deltatable.sh && ./run_sensor_to_deltatable.sh',
    cmd_timeout=1800,
    dag=sensor_dag
)

sensor_end = DummyOperator(task_id='pipeline_complete', dag=sensor_dag)
sensor_start >> sensor_kafka_minio >> sensor_bronze >> sensor_silver >> sensor_gold >> sensor_end

# =============================================================================
# 4. SOCIAL PIPELINE - Runs at 06:00 daily (2 hours after Sensor)
# =============================================================================
social_dag = DAG(
    'social_delta_table_pipeline',
    default_args=default_args,
    description='Social to Delta Table processing pipeline: Kafka-MinIO -> Bronze -> Silver -> Gold',
    schedule_interval='0 6 * * *',  # Daily at 06:00
    tags=['delta-lake', 'social', 'etl', 'kafka-minio'],
    max_active_runs=1
)

social_start = DummyOperator(task_id='start_pipeline', dag=social_dag)

social_kafka_minio = NoTemplateSSHOperator(
    task_id='kafka_minio_social_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/kafka_minio_Multiple_pipes && chmod +x run_kafka_minio_post.sh && ./run_kafka_minio_post.sh',
    cmd_timeout=1800,
    dag=social_dag
)

social_bronze = NoTemplateSSHOperator(
    task_id='bronze_social_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/bronze && chmod +x run_social_to_deltatable.sh && ./run_social_to_deltatable.sh',
    cmd_timeout=1800,
    dag=social_dag
)

social_silver = NoTemplateSSHOperator(
    task_id='silver_social_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/silver && chmod +x run_social_to_deltatable.sh && ./run_social_to_deltatable.sh',
    cmd_timeout=1800,
    dag=social_dag
)

social_gold = NoTemplateSSHOperator(
    task_id='gold_social_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/gold && chmod +x run_social_to_deltatable.sh && ./run_social_to_deltatable.sh',
    cmd_timeout=1800,
    dag=social_dag
)

social_end = DummyOperator(task_id='pipeline_complete', dag=social_dag)
social_start >> social_kafka_minio >> social_bronze >> social_silver >> social_gold >> social_end

# =============================================================================
# 5. DATA WAREHOUSE PIPELINE - Runs at 08:00 daily (after all data processing is complete)
# =============================================================================
dw_dag = DAG(
    'data_warehouse_pipeline',
    default_args=default_args,
    description='Data Warehouse processing pipeline - aggregates all delta tables',
    schedule_interval='0 8 * * *',  # Daily at 08:00 AM
    tags=['delta-lake', 'data-warehouse', 'etl'],
    max_active_runs=1
)

dw_start = DummyOperator(task_id='start_pipeline', dag=dw_dag)

dw_processing = NoTemplateSSHOperator(
    task_id='data_warehouse_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/DW && chmod +x run_DW.sh && ./run_DW.sh',
    cmd_timeout=3600,  # 1 hour timeout for DW processing
    dag=dw_dag
)

# Add time dimension processing
time_processing = NoTemplateSSHOperator(
    task_id='time_dimension_processing',
    ssh_conn_id=SSH_CONN_ID,
    command='cd ~/runners/delta_table/gold && chmod +x run_time.sh && ./run_time.sh',
    cmd_timeout=1800,  # 30 minutes timeout
    dag=dw_dag
)

dw_end = DummyOperator(task_id='pipeline_complete', dag=dw_dag)
dw_start >> [dw_processing, time_processing] >> dw_end