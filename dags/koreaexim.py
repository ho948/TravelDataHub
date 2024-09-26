import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(hours=24),
}

# Define the DAG
dag = DAG(
    'koreaexim_dag_1',
    default_args=default_args,
    description='환율 정보 ETL DAG',
    schedule_interval='@daily',
    catchup=True
)

# 쉼표가 있는 숫자 문자열을 float 숫자로 변환하는 함수
def convert_to_float(num_str):
    return float(num_str.replace(',', ''))

# API를 호출하여 값을 추출하는 함수
def extract(**context):
    logging.info("Extract is Started")

    params = {
        'authkey': Variable.get("KOREAEXIM_API_KEY"),
        'searchdate': context['execution_date'].isoformat().split("T")[0],
        'data': 'AP01'
    }
    api_url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON"
    response = requests.get(api_url, params=params, verify=False)
    response.raise_for_status()
    data = response.json()

    logging.info("Extract is Successed")
    
    return data

# API 결과값을 변환 및 적재하는 함수
def transform_and_load(**context):
    logging.info("Transform and Load are Started")

    # task_ids가 extract인 테스크를 읽어서 해당 테스크의 리턴값을 저장
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    # Database connection
    pg_hook = PostgresHook(postgres_conn_id="gcp_postgresql_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    search_date = context['execution_date'].isoformat()
    try:
        for d in data:
            cur_unit = d['cur_unit']
            cur_nm = d['cur_nm']
            ttb = convert_to_float(d['ttb'])
            tts = convert_to_float(d['tts'])
            deal_bas_r = convert_to_float(d['deal_bas_r'])
            bkpr = convert_to_float(d['bkpr'])
            yy_efee_r = convert_to_float(d['yy_efee_r'])
            ten_dd_efee_r = convert_to_float(d['ten_dd_efee_r'])
            kftc_deal_bas_r = convert_to_float(d['kftc_deal_bas_r'])
            kftc_bkpr = convert_to_float(d['kftc_bkpr'])

            # Insert or update unit table
            cursor.execute("""
                INSERT INTO my_project_1.raw_data.unit (unit_code, unit_name)
                VALUES (%s, %s)
                ON CONFLICT (unit_code) DO UPDATE SET unit_name = EXCLUDED.unit_name
                RETURNING id;
            """, (cur_unit, cur_nm))
            
            unit_id = cursor.fetchone()[0]

            # Insert or update exchange table
            cursor.execute("""
                INSERT INTO my_project_1.raw_data.exchange (
                    unit_id, ttb, tts, deal_bas_r, bkpr, yy_efee_r, ten_dd_efee_r, kftc_deal_bas_r, kftc_bkpr, cur_nm, execution_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (unit_id, ttb, tts, deal_bas_r, bkpr, yy_efee_r, ten_dd_efee_r, kftc_deal_bas_r, kftc_bkpr, cur_nm, search_date))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
    
    logging.info("Transform and Load are Successed")

# Define the task
extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_and_load = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    dag=dag,
)

# Set task dependencies
extract >> transform_and_load