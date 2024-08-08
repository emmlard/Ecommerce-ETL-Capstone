from airflow.decorators import dag
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
from airflow.operators.dummy_operator import DummyOperator 
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.baseoperator import chain

# Define the default arguments for the DAG
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 0,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=2)  # Time between retries
}

# Access Airflow Variables
PG_CONN = "myPostgresConnection"
GCS_CONN = "myGcsConnection"
BQ_PROJECT = "automatic-asset"
BQ_DATASET = "ecommerce_raw"
SCHEMA = "raw"
SCHEMA_PATH = "/usr/local/airflow/include/schema"

## Function to json to variable
def read_json(path: str):
    try:
        with open(path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception(f"File not found: {path}")
    except json.JSONDecodeError:
        raise Exception(f"Error decoding JSON in file: {path}")

# Create a DAG instance
@dag(
    dag_id='BigQuery_Ecommerce',
    default_args=default_args,
    description='An Airflow DAG for historical loading of Ecommerce Postgres data to Bigquery',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False,  # Do not backfill (run past dates) when starting the DAG
    tags=['extraction', 'postgres', 'gcs']
)

def pg_to_bq():

    # Define start dummy task
    start_task = DummyOperator(
        task_id='start'
    )

    ### Task to export order_items table to csv.gz in gcs
    export_pg_data_to_gcs_order_items = PostgresToGCSOperator(
        task_id='export_order_items_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.order_items",
        bucket="ecom-raw",
        filename='order_items.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )
    ### Task to export order_payments table to csv.gz in gcs
    export_pg_data_to_gcs_order_payments = PostgresToGCSOperator(
        task_id='export_order_payments_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.order_payments",
        bucket="ecom-raw",
        filename='order_payments.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )
    ### Task to export geolocation table to csv.gz in gcs
    export_pg_data_to_gcs_geolocation = PostgresToGCSOperator(
        task_id='export_geolocation_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.geolocation",
        bucket="ecom-raw",
        filename='geolocation.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )
    
    ### Task to export order_reviews table to csv.gz in gcs 
    export_pg_data_to_gcs_order_reviews = PostgresToGCSOperator(
        task_id='export_order_reviews_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.order_reviews",
        bucket="ecom-raw",
        filename='order_reviews.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ### Task to export product_category_name_translation table to csv.gz in gcs
    export_pg_data_to_gcs_product_category_name_translation = PostgresToGCSOperator(
        task_id='export_product_category_name_translation_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.product_category_name_translation",
        bucket="ecom-raw",
        filename='product_category_name_translation.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ### Task to export sellers table to csv.gz in gcs
    export_pg_data_to_gcs_sellers = PostgresToGCSOperator(
        task_id='export_sellers_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.sellers",
        bucket="ecom-raw",
        filename='sellers.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ### Task to export products table to csv.gz in gcs
    export_pg_data_to_gcs_products = PostgresToGCSOperator(
        task_id='export_products_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.products",
        bucket="ecom-raw",
        filename='products.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ### Task to export customers table to csv.gz in gcs
    export_pg_data_to_gcs_customers = PostgresToGCSOperator(
        task_id='export_customers_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.customers",
        bucket="ecom-raw",
        filename='customers.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ### Task to export orders table to csv.gz in gcs
    export_pg_data_to_gcs_orders = PostgresToGCSOperator(
        task_id='export_orders_to_csv_gcs',
        sql=f"SELECT * FROM {SCHEMA}.orders",
        bucket="ecom-raw",
        filename='orders.csv.gz',
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        gzip=True,
        use_server_side_cursor=False,
    )

    ## Task to load order_items.csv.gz from GCS Bucket to BigQuery
    order_items_to_bq = GCSToBigQueryOperator(
        task_id="export_order_items_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'order_items.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_items",
        schema_fields=read_json(f"{SCHEMA_PATH}/order_items.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load order_payments.csv.gz from GCS Bucket to BigQuery
    order_payments_to_bq = GCSToBigQueryOperator(
        task_id="export_order_payments_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'order_payments.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_payments",
        schema_fields=read_json(f"{SCHEMA_PATH}/order_payments.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load geolocation.csv.gz from GCS Bucket to BigQuery
    geolocation_to_bq = GCSToBigQueryOperator(
        task_id="export_geolocation_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'geolocation.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.geolocation",
        schema_fields=read_json(f"{SCHEMA_PATH}/geolocation.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load order_reviews.csv.gz from GCS Bucket to BigQuery
    order_reviews_to_bq = GCSToBigQueryOperator(
        task_id="export_order_reviews_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'order_reviews.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_reviews",
        schema_fields=read_json(f"{SCHEMA_PATH}/order_reviews.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load product_category_name_translation.csv.gz from GCS Bucket to BigQuery
    product_category_name_translation_to_bq = GCSToBigQueryOperator(
        task_id="export_product_category_name_translation_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'product_category_name_translation.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.product_category_name_translation",
        schema_fields=read_json(f"{SCHEMA_PATH}/product_category_name_translation.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load sellers.csv.gz from GCS Bucket to BigQuery
    sellers_to_bq = GCSToBigQueryOperator(
        task_id="export_sellers_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'sellers.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.sellers",
        schema_fields=read_json(f"{SCHEMA_PATH}/sellers.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load products.csv.gz from GCS Bucket to BigQuery
    products_to_bq = GCSToBigQueryOperator(
        task_id="export_products_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'products.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.products",
        schema_fields=read_json(f"{SCHEMA_PATH}/products.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load customers.csv.gz from GCS Bucket to BigQuery
    customers_to_bq = GCSToBigQueryOperator(
        task_id="export_customers_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'customers.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.customers",
        schema_fields=read_json(f"{SCHEMA_PATH}/customers.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load orders.csv.gz from GCS Bucket to BigQuery
    orders_to_bq = GCSToBigQueryOperator(
        task_id="export_orders_to_bigquery",
        bucket="ecom-raw",
        source_objects=[f'orders.csv.gz'],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.orders",
        schema_fields=read_json(f"{SCHEMA_PATH}/orders.json"),
        source_format='CSV',
        compression='GZIP',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    # Define start and end dummy task
    end_task = DummyOperator(
        task_id='end'
    )

    chain(
        start_task,
        export_pg_data_to_gcs_order_items, order_items_to_bq,
        export_pg_data_to_gcs_order_payments, order_payments_to_bq,
        export_pg_data_to_gcs_geolocation, geolocation_to_bq,
        export_pg_data_to_gcs_order_reviews, order_reviews_to_bq,
        export_pg_data_to_gcs_product_category_name_translation, product_category_name_translation_to_bq,
        export_pg_data_to_gcs_sellers, sellers_to_bq,
        export_pg_data_to_gcs_products, products_to_bq,
        export_pg_data_to_gcs_customers, customers_to_bq,
        export_pg_data_to_gcs_orders, orders_to_bq,
        end_task
    )

pg_to_bq()