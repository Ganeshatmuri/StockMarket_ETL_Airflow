from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta,datetime
import yfinance as yf
from airflow.providers.postgres.hooks.postgres import PostgresHook



default_args={
    'owner':'ganesh',
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

def stock_name():
    return "Reliance"

def get_stock_data(ti):
# Create ticker object
    stock_name=ti.xcom_pull(task_ids='stock_name_task')
    ticker = yf.Ticker(f"{stock_name}.NS")
    info = ticker.info
    return info

def transform_stock_data(ti):
    info=ti.xcom_pull(task_ids='get_stock_data')
    print("previewing data before transformation")
    print(info)
    print("previewing data after transformation")
    transformed_data={
        'company':info['longName'],
        'industry':info['industry'],
        'last_price':info['regularMarketPrice'],
        'open':info['regularMarketOpen'],
        'high':info['dayHigh'],
        'low':info['dayLow'],
        '52_week_high_range':info['fiftyTwoWeekRange']
    }
    return transformed_data

def load_stock_data(ti):
    transformed_data = ti.xcom_pull(task_ids='transform_stock_data')

    pg_hook = PostgresHook(postgres_conn_id="postgres_stock")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data(
            company VARCHAR,
            industry VARCHAR,
            last_price FLOAT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            week_52_high_range VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("""
        INSERT INTO stock_data(
            company,industry,last_price,open,high,low,week_52_high_range
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s)
    """, (
        transformed_data['company'],
        transformed_data['industry'],
        transformed_data['last_price'],
        transformed_data['open'],
        transformed_data['high'],
        transformed_data['low'],
        transformed_data['52_week_high_range']
    ))

    conn.commit()
    cursor.close()
    conn.close()
    
with DAG(
    default_args=default_args,
    dag_id="Indian_stock_mkt",
    description="Indian stock market details",
    start_date=datetime(2026,4,8),
    schedule='@daily'
) as dag:
    task1=PythonOperator(
        task_id='stock_name_task',
        python_callable=stock_name
    )
    task1
    task2=PythonOperator(
        task_id='get_stock_data',
        python_callable=get_stock_data
    )
    task3=PythonOperator(
        task_id='transform_stock_data',
        python_callable=transform_stock_data
    )
    task4=PythonOperator(
        task_id='load_stock_data',
        python_callable=load_stock_data
    )
    task1>>task2>>task3>>task4