from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag
from airflow import DAG
import os
import pandas as pd

default_args = {
    'owner': 'PhucNguyen',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def data_extract(ti):
    # Get the directory of the current DAG file
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))

    # Save to CSV file in the specified output directory
    raw_data_path = os.path.join(current_dag_directory, "data/raw/")

    # Check raw data file csv
    found_files = False
    for dirname, _, filenames in os.walk(raw_data_path):
        for filename in filenames:
            csv_path = os.path.join(dirname, filename)
            found_files = True
            print(csv_path)
    
    if found_files:
        data = pd.read_csv(csv_path, encoding='windows-1254')
        ti.xcom_push(key='raw_data', value=data)
    else:
        print(f"Don't have any file in '{raw_data_path}' folder!!!")

def customer_data_transform(ti):
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data_task')

    df_customer = raw_data[['CustomerID','Country']]

    ### Data Info Check
    print('Dataframe dimensions:',df_customer.shape)
    print('Information of data: ', df_customer.info())

    ### Missing value analysis
    print('Missing value: ', df_customer.isna().sum())

    # Delete Null value
    df_customer = df_customer.dropna()

    # Delete Duplicate value
    df_customer = df_customer.drop_duplicates(subset=['CustomerID'], keep='last')
    
    # Change data type to int, string
    df_customer['CustomerID'] = df_customer['CustomerID'].astype(int)
    df_customer['Country'] = df_customer['Country'].astype('string')

    # Data type info
    print('Information of data: ', df_customer.info())
    print('Data type: ', df_customer.dtypes)

    ti.xcom_push(key='transformed_customer_data', value=df_customer)

def date_data_transform(ti):
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data_task')

    df_date = raw_data[['InvoiceDate']]

    ### Data Info Check
    print('Dataframe dimensions:',df_date.shape)
    print('Information of data: ', df_date.info())
    
    ### Missing value analysis
    print('Missing value: ', df_date.isna().sum())
    print(df_date.head(5))
    
    # Delete Duplicate value
    df_date = df_date.drop_duplicates()

    print('Information of data: ', df_date.info())

    #Rename
    df_date = df_date.rename(columns={"InvoiceDate": "datetime_id"})

    # Add datetime_id column format YYYYmmddHHMM
    df_date['datetime_id'] = pd.to_datetime(df_date['datetime_id']).dt.strftime('%Y%m%d%H%M')

    # Change data type and rename column
    df_date['datetime'] = pd.to_datetime(df_date['datetime_id']).dt.strftime('%Y-%m-%d-%H:%M')
    
    #Change datatime type
    df_date['datetime_id'] = df_date['datetime_id'].astype('string')
    df_date['datetime'] = pd.to_datetime(df_date['datetime'], errors='coerce')
    

    print(df_date.head(5))
    print(df_date.info())

    # Add other columns
    df_date['year'] = df_date['datetime'].dt.year
    df_date['quarter'] = df_date["datetime"].dt.quarter
    df_date['month'] = df_date["datetime"].dt.month
    df_date['day'] = df_date["datetime"].dt.day
    df_date["hour"] = df_date["datetime"].dt.hour
    df_date["min"] = df_date["datetime"].dt.minute
    df_date["day_name"] = df_date["datetime"].dt.day_name()

    print(df_date.info())

    ti.xcom_push(key='transformed_date_data', value=df_date)

def product_data_transform(ti):
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data_task')

    df_product = raw_data[['StockCode', 'Description', 'UnitPrice']]

    ### Data Info Check
    print('Dataframe dimensions:',df_product.shape)
    print('Information of data: ', df_product.info())

    ### Missing value analysis
    print('Missing value: ', df_product.isna().sum())

    df_product = df_product[df_product['UnitPrice'] > 0 ]

    df_product = df_product[['StockCode', 'Description']]

    # Delete Duplicate value subset=['StockCode']
    df_product = df_product.drop_duplicates(subset=['StockCode'], keep='last')

    # Xóa dấu móc đơn
    df_product['Description'] = df_product['Description'].str.replace("'", "", regex=False)

    # Xoa khoang trang thua
    # df_product['Description'] = df_product['Description'].strip()

    ### Data Info Check
    print('Dataframe dimensions:',df_product.shape)
    print('Information of data: ', df_product.info())

    ### Missing value analysis
    print('Missing value: ', df_product.isna().sum())

    ti.xcom_push(key='transformed_product_data', value=df_product)

def insert_data_table(df, check_query, insert_query):
    postgres_hook = PostgresHook(postgres_conn_id="dwh_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    try:
        count = 0
        for i, row in df.iterrows():         
            para = tuple(row)
            id = para[0]

            # Check if the record exists (using id)
            cur.execute(check_query, (id,))
            exists = cur.fetchone()[0]

            if not exists:
                count += 1
                cur.execute(insert_query % para)
                # Committing all transactions to ensure that the changes are saved
                conn.commit()
            
        print("New data inserted ", count, " rows successfully")
    except Exception as e:
        print("Error while inserting data table: " + str(e))

    cur.close()
    conn.close()

def customer_data_load(ti):
    df_customer = ti.xcom_pull(key='transformed_customer_data', task_ids='transform_customer_data_task')
    print(df_customer.info())
    check_query = """
            SELECT EXISTS (SELECT 1 FROM dim_customer WHERE customer_id = %s)
            """
    insert_query = """
            INSERT INTO dim_customer ("customer_id", "country")
                            VALUES (%s, '%s');
            """
    insert_data_table(df_customer, check_query, insert_query)

def date_data_load(ti):
    df_date = ti.xcom_pull(key='transformed_date_data', task_ids='transform_date_data_task')
    print(df_date.info())
    check_query = """
            SELECT EXISTS (SELECT 1 FROM dim_date WHERE datetime_id = %s)
            """
    insert_query = """
            INSERT INTO dim_date ("datetime_id", "datetime", "year", "quarter", "month", "day", "hour", "minute", "day_name")
                            VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s, '%s');
            """
    insert_data_table(df_date, check_query, insert_query)

def product_data_load(ti):
    df_product = ti.xcom_pull(key='transformed_product_data', task_ids='transform_product_data_task')
    print(df_product.info())
    check_query = """
            SELECT EXISTS (SELECT 1 FROM dim_product WHERE product_id = %s)
            """
    insert_query = """
            INSERT INTO dim_product ("product_id", "description")
                            VALUES ('%s', '%s');
            """
    insert_data_table(df_product, check_query, insert_query)

def invoices_data_load(ti):
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data_task')
    df_invoices = raw_data[['InvoiceNo', 'InvoiceDate', 'StockCode', 'CustomerID', 'Quantity', 'UnitPrice']]

    df_invoices['InvoiceDate'] = pd.to_datetime(df_invoices['InvoiceDate']).dt.strftime('%Y%m%d%H%M')

    # Delete Duplicate value
    # Nhóm dữ liệu và tính tổng quantity
    df_invoices = df_invoices.groupby(['InvoiceNo', 'InvoiceDate', 'StockCode', 'CustomerID', 'UnitPrice'])['Quantity'].sum().reset_index()

    print(df_invoices.info())

    postgres_hook = PostgresHook(postgres_conn_id="dwh_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()    
    
    try:
        count = 0
        for i, row in df_invoices.iterrows():
            para = tuple(row)
            invoice_id = para[0]
            datetime_id = para[1]
            product_id = para[2]
            customer_id = para[3]
            price = para[4]
            quantity = para[5]
            total = quantity*price

            if customer_id > 0 and quantity > 0 and price > 0:
                check_query = """
                            SELECT EXISTS (
                            SELECT 1 FROM dim_date WHERE datetime_id = '%s'
                            ) AND EXISTS (
                                SELECT 1 FROM dim_product WHERE product_id = '%s'
                            ) AND EXISTS (
                                SELECT 1 FROM dim_customer WHERE customer_id = %s
                            )
                            """
                cur.execute(check_query % (datetime_id, product_id, customer_id))
                result = cur.fetchone()

                if result and result[0]:
                    count += 1
                    insert_query = """
                                INSERT INTO fact_invoices ("invoice_id", "datetime_id", "product_id", "customer_id", "quantity", "price", "total")
                                                            VALUES ('%s', '%s', '%s', %s, %s, %s, %s);                        
                                """
                    cur.execute(insert_query % (invoice_id, datetime_id, product_id, customer_id, quantity, price, total))
                    # Committing all transactions to ensure that the changes are saved
                    conn.commit()
        print("Data inserted ", count, " rows successfully")
    except Exception as e:
        print("Error while inserting data table: " + str(e))

    cur.close()
    conn.close()

with DAG(
    dag_id='online_retail_dwh_dag_v31',
    default_args=default_args,
    description='DAG to extract, transform and load data to Online Retail Sale Data Warehouse',
    start_date=datetime(2025, 3, 5, 2),
    schedule_interval='@daily'
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=data_extract
    )
    # Trasform Tasks
    customer_data_transform_task = PythonOperator(
        task_id='transform_customer_data_task',
        python_callable=customer_data_transform
    )
    date_data_transform_task = PythonOperator(
        task_id='transform_date_data_task',
        python_callable=date_data_transform
    )
    product_data_transform_task = PythonOperator(
        task_id='transform_product_data_task',
        python_callable=product_data_transform
    )
    
    # Create Tables
    create_dim_customer_table_task = PostgresOperator(
        task_id="create_dim_customer_table",
        postgres_conn_id="dwh_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS dim_customer (
                "customer_id" INT,
                "country" VARCHAR(30),
                PRIMARY KEY ("customer_id")
            );"""
    )
    create_dim_date_table_task = PostgresOperator(
        task_id="create_dim_date_table",
        postgres_conn_id="dwh_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS dim_date (
                "datetime_id" VARCHAR(15),
                "datetime" TIMESTAMP,
                "year" INT,
                "quarter" INT,
                "month" INT,
                "day" INT,
                "hour" INT,
                "minute" INT,
                "day_name" VARCHAR(10),
                PRIMARY KEY ("datetime_id")
            );"""
    )
    create_dim_product_table_task = PostgresOperator(
        task_id="create_dim_product_table",
        postgres_conn_id="dwh_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS dim_product (
                "product_id" VARCHAR(15),
                "description" VARCHAR(40),
                PRIMARY KEY ("product_id")
            );"""
    )
    create_fact_invoices_table_task = PostgresOperator(
        task_id="create_fact_invoices_table",
        postgres_conn_id="dwh_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS fact_invoices (
                "invoice_key" SERIAL,
                "invoice_id" VARCHAR(10),
                "datetime_id" VARCHAR(15),
                "product_id" VARCHAR(15),
                "customer_id" INT,
                "quantity" INT,
                "price" FLOAT,
                "total" FLOAT,
                PRIMARY KEY ("invoice_key"),
                FOREIGN KEY(datetime_id) REFERENCES dim_date(datetime_id),
                FOREIGN KEY(product_id) REFERENCES dim_product(product_id),
                FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id)
            );"""
    )

    # Load Tasks
    customer_data_load_task = PythonOperator(
        task_id='load_customer_data_task',
        python_callable=customer_data_load
    )
    date_data_load_task = PythonOperator(
        task_id='load_date_data_task',
        python_callable=date_data_load
    )
    product_data_load_task = PythonOperator(
        task_id='load_product_data_task',
        python_callable=product_data_load
    )
    invoices_fact_load_task = PythonOperator(
        task_id='load_invoices_data_task',
        python_callable=invoices_data_load
    )
    extract_task >> [customer_data_transform_task, date_data_transform_task, product_data_transform_task]
    [customer_data_transform_task >> create_dim_customer_table_task >> customer_data_load_task,
     date_data_transform_task >> create_dim_date_table_task >> date_data_load_task,
     product_data_transform_task >> create_dim_product_table_task >> product_data_load_task] >> create_fact_invoices_table_task >> invoices_fact_load_task