from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hdfs_operations import HdfsMkdirFileOperator, HdfsPutFileOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

from custom_operators import CreateDirectoryOperatorPathlib, RemoveDirectoryOperator


MTG_DIRECTORY = '/home/airflow/mtg/'
DOWNLOAD_URI_CSV = 'https://mtgjson.com/api/v5/AllPrintingsCSVFiles.tar.gz'

# -------------------------------------------------
# Dag
# -------------------------------------------------

args = {
    'owner': 'airflow'
}

dag = DAG(
    'MTG_CSV',
    default_args=args,
    description='MTG Import',
    schedule_interval='56 18 * * *',
    start_date=datetime(2021, 11, 3),
    catchup=False,
    max_active_runs=1
)


# -------------------------------------------------
# Create/Delete Connections
# -------------------------------------------------

create_hive_connection_command = '''
    airflow connections -a \
        --conn_id 'hiveserver2_connection_big_data' \
        --conn_type 'hiveserver2' \
        --conn_login 'hadoop' \
        --conn_password '' \
        --conn_host 'hadoop' \
        --conn_port '10000' \
        --conn_schema 'default'
'''

create_mysql_connection_command = '''
    airflow connections -a \
        --conn_id 'mysql_connection_big_data' \
        --conn_type 'mysql' \
        --conn_login 'root' \
        --conn_password 'tinf19d2021' \
        --conn_host 'uldz7bsq6x3frltd.myfritz.net' \
        --conn_port '3308' \
        --conn_schema 'big_data' \
        --conn_extra '{"charset": "utf8"}'
'''

create_hive_connection = BashOperator(
    task_id='create_hive_connection',
    bash_command=create_hive_connection_command,
    dag=dag,
)

create_mysql_connection = BashOperator(
    task_id='create_mysql_connection',
    bash_command=create_mysql_connection_command,
    dag=dag,
)

delete_hive_connection = BashOperator(
    task_id='delete_hive_connection',
    bash_command='airflow connections -d --conn_id hiveserver2_connection_big_data',
    dag=dag,
)

delete_mysql_connection = BashOperator(
    task_id='delete_mysql_connection',
    bash_command='airflow connections -d --conn_id mysql_connection_big_data',
    dag=dag,
)


# -------------------------------------------------
# Create/Remove Import Directory
# -------------------------------------------------

create_local_dirs = CreateDirectoryOperatorPathlib(
    task_id='create_local_dirs',
    directory=f'{MTG_DIRECTORY}data',
    dag=dag,
)

remove_local_dir = RemoveDirectoryOperator(
    task_id='remove_local_dir',
    directory=f'{MTG_DIRECTORY}data',
    dag=dag,
)


# -------------------------------------------------
# Download & Unzip data
# -------------------------------------------------

download_mtg_cards_csv = BashOperator(
    task_id='download_mtg_cards_csv',
    bash_command=f'wget {DOWNLOAD_URI_CSV} -P {MTG_DIRECTORY}data/',
    dag=dag,
)

unzip_mtg_cards_csv = BashOperator(
    task_id='unzip_mtg_cards_csv',
    bash_command=f'tar -zxvf {MTG_DIRECTORY}data/AllPrintingsCSVFiles.tar.gz -C {MTG_DIRECTORY}data',
    dag=dag,
)


# -------------------------------------------------
# HDFS
# -------------------------------------------------

hdfs_mkdir_mtg_raw = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_mtg_raw',
    directory='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_mkdir_mtg_final = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_mtg_final',
    directory='/user/hadoop/mtg/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_mtg_cards = HdfsPutFileOperator(
    task_id='hdfs_put_mtg_cards',
    local_file=f'{MTG_DIRECTORY}data/AllPrintingsCSVFiles/cards.csv',
    remote_file='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/cards_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)


# -------------------------------------------------
# PySpark manipulate CSV
# -------------------------------------------------

pyspark_mtg_cards = SparkSubmitOperator(
    task_id='pyspark_write_cleansed_mtg_cards_to_hdfs',
    conn_id='spark',
    application='/home/airflow/airflow/dags/pyspark_mtg.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='4g',
    num_executors='2',
    name='spark_cleansing_csv',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/mtg/raw/', '--hdfs_target_dir', '/user/hadoop/mtg/final/', '--hdfs_target_format', 'csv'],
    dag=dag,
)


# -------------------------------------------------
# HiveSQL
# -------------------------------------------------

hiveSQL_create_table_mtg_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_cards(
    id STRING,
    artist STRING,
    availability array<STRING>,
    borderColor STRING,
    colors array<STRING>,
    convertedManaCost BIGINT,
    edhrecRank BIGINT,
    finishes array<STRING>,
    flavorText STRING,
    keywords array<STRING>,
    layout STRING,
    manaCost STRING,
    manaValue STRING,
    multiverseId STRING,
    name STRING,
    number STRING,
    power STRING,
    rarity STRING,
    setCode STRING,
    subtypes array<STRING>,
    supertypes array<STRING>,
    text STRING,
    toughness STRING,
    type STRING,
    types array<STRING>
) COMMENT 'Magic the Gathering Cards' PARTITIONED BY (partition_year int, partition_month int, partition_day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0023'
STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/cards'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_add_partition_mtg_cards='''
ALTER TABLE mtg_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/final/pyspark_mtg_cards.csv';
'''

drop_HiveTable_mtg_cards = HiveOperator(
    task_id='drop_mtg_cards_table',
    hql='DROP TABLE IF EXISTS default.mtg_cards',
    hive_cli_conn_id='beeline',
    dag=dag,
)

create_HiveTable_mtg_cards = HiveOperator(
    task_id='create_mtg_cards_table',
    hql=hiveSQL_create_table_mtg_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
)

addPartition_HiveTable_mtg_cards = HiveOperator(
    task_id='add_partition_mtg_cards_table',
    hql=hiveSQL_add_partition_mtg_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
)


# -------------------------------------------------
# MySQL
# -------------------------------------------------

mysql_create_table = '''
CREATE TABLE IF NOT EXISTS mtg_cards_jakob(
    id TEXT,
    artist TEXT,
    availability TEXT,
    borderColor TEXT,
    colors TEXT,
    convertedManaCost INT,
    edhrecRank INT,
    finishes TEXT,
    flavorText TEXT,
    keywords TEXT,
    layout TEXT,
    manaCost TEXT,
    manaValue TEXT,
    multiverseId TEXT,
    name TEXT,
    number TEXT,
    power TEXT,
    rarity TEXT,
    setCode TEXT,
    subtypes TEXT,
    supertypes TEXT,
    text TEXT,
    toughness TEXT,
    type TEXT,
    types TEXT,
    partition_year INT,
    partition_month INT,
    partition_day INT
);
'''

mysql_truncate_table = '''
    TRUNCATE mtg_cards_jakob;
'''

create_remote_mysql_table = MySqlOperator(
    task_id='create_remote_mysql_table',
    sql=mysql_create_table,
    mysql_conn_id='mysql_connection_big_data',
    database='big_data',
    dag=dag,
)

truncate_remote_mysql_table = MySqlOperator(
    task_id='truncate_remote_mysql_table',
    sql=mysql_truncate_table,
    mysql_conn_id='mysql_connection_big_data',
    database='big_data',
    dag=dag,
)


# -------------------------------------------------
# Hive to MySQL
# -------------------------------------------------

#
hive_to_mysql = HiveToMySqlTransfer(
    task_id='hive_to_mysql',
    sql="SELECT * FROM mtg_cards WHERE id <= 1000",
    hiveserver2_conn_id='hiveserver2_connection_big_data',
    mysql_table='mtg_cards_jakob',
    mysql_conn_id='mysql_connection_big_data',
    dag=dag,
)


# -------------------------------------------------
# Task Order
# -------------------------------------------------

create_hive_connection >> create_local_dirs
create_mysql_connection >> create_local_dirs

create_local_dirs >> download_mtg_cards_csv

download_mtg_cards_csv >> unzip_mtg_cards_csv

unzip_mtg_cards_csv >> hdfs_mkdir_mtg_raw >> hdfs_put_mtg_cards
unzip_mtg_cards_csv >> hdfs_mkdir_mtg_final >> hdfs_put_mtg_cards

hdfs_put_mtg_cards >> pyspark_mtg_cards

pyspark_mtg_cards >> drop_HiveTable_mtg_cards >> create_HiveTable_mtg_cards >> addPartition_HiveTable_mtg_cards
pyspark_mtg_cards >> create_remote_mysql_table >> truncate_remote_mysql_table

addPartition_HiveTable_mtg_cards >> hive_to_mysql
truncate_remote_mysql_table >> hive_to_mysql

hive_to_mysql >> delete_hive_connection
hive_to_mysql >> delete_mysql_connection
hive_to_mysql >> remove_local_dir
