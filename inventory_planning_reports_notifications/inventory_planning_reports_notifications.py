"""
DAG to check reports update status
"""
import json
import logging
import os
import warnings
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from requests_ntlm import HttpNtlmAuth

warnings.simplefilter(action="ignore")

logger = logging.getLogger(__file__)

PATH, FILENAME = os.path.split(os.path.abspath(__file__))
QUERIES_PATH = os.path.join(PATH, "queries")
# Batch processing module name
MODULE_NAME = "replenishment"
# Set dag id as module name + current filename
DAG_ID = (
    MODULE_NAME
    + "__"
    + os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
)

args = {
    "owner": "me",
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "start_date": datetime(2024, 5, 5),
    "email_on_retry": False,
    "email_on_failure": True,
    "email": ["my email"],
    "queue": MODULE_NAME,
}

dag = DAG(
    dag_id=MODULE_NAME
    + "__"
    + os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    default_args=args,
    max_active_runs=1,
    schedule_interval="0 6 * * 1-5",
    tags=["replenishment", "almaz"],
    access_control={"replenishment": {"can_edit", "can_delete"}}
)


def load_json_data_from_file(file_name: str, folder_path: str):
    """
    Reads json data from file
    """
    fullpath = os.path.join(folder_path, file_name)
    with open(fullpath) as f:
        data = json.load(f)
    return data


def get_dag_settings(folder_path: str):
    """
    Reads json file dag_settings.json
    """
    return load_json_data_from_file(
        file_name="dag_settings.json", folder_path=folder_path
    )


def select_query(sql: str, conn_id: str) -> list:
    """Returns first row from a query in source db"""
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
    except Exception as exc:
        logger.info(exc)
        result = False
    return result


def get_mart_update_date(sql: str, conn_id: str) -> datetime:
    """Returns table update date"""
    result = select_query(sql, conn_id)
    if result == False:
        updated_dttm = datetime(1900, 1, 1, 0, 0)
    else:
        updated_dttm = result[0]
    return updated_dttm


def read_query_from_file(query_path: str) -> str:
    """Reads sql file and returns query as a string"""
    with open(query_path, "r") as query:
        query = query.read()
    return query


def get_dept_details(reports_auth, reports_dept):
    from requests_ntlm import HttpNtlmAuth
    """Returns token and chat_id for the specified department chatbot"""
    #logger.info(f"reports_auth var {reports_auth}, type {type(reports_auth)}, length {len(reports_auth)}")
    #logger.info(f"reports dept var {reports_dept}, type {type(reports_dept)}")
    reports_auth_json=json.loads(reports_auth.strip().replace("\'{","{").replace("}\'","}"))
    my_token = reports_auth_json[reports_dept]["tg_bot"]["my_token"]
    my_chat_id = reports_auth_json[reports_dept]["tg_bot"]["my_chat_id"]
    my_login = reports_auth_json[reports_dept]["power_bi"]["my_login"]
    my_password = reports_auth_json[reports_dept]["power_bi"]["my_password"]
    # auth
    auth = HttpNtlmAuth(my_login, my_password)
    return my_token, my_chat_id, auth


def get_report_details(report_details):
    """Returns name_id of a specified report and conn_id and query_path to query update_dttm"""
    conn_id = False
    query_path = False
    logger.info(f"report details {report_details}")
    name_id = report_details["name_id"]
    if "connection" in report_details:
        conn_id = report_details["connection"]
    if "query" in report_details:
        query_path = QUERIES_PATH + report_details["query"]
    return name_id, conn_id, query_path


def report_api_refresh_telegram(
    report_details: dict, my_token: str, my_chat_id: str, auth: str
):
    """Returns update status of the specified report and source mart update date"""
    report_name = report_details["name_rus"]
    print(f"get update status and main mart update date for {report_name}")
    name_id, conn_id, query_path = get_report_details(report_details)

    # reads API update data. Details https://app.swaggerhub.com/apis/microsoft-rs/PBIRS/2.0
    # API may return the last 10 updates
    url = f"https://powerbi.data.lmru.tech//reports/api/v2.0/CacheRefreshPlans({name_id})/History"
    response = requests.get(url, auth=auth, verify=False)
    resp_json = response.json()

    # dict with attributes to get
    pd_dict = {
        "StartTime": [],
        "Status": [],
        "Message": [],
    }

    # get the necessary attributes
    for record in resp_json["value"]:
        pd_dict["StartTime"].append(record["StartTime"])
        pd_dict["Status"].append(record["Status"])
        pd_dict["Message"].append(record["Message"])

    start_time = datetime.strptime(
        pd_dict["StartTime"][0][:19], "%Y-%m-%dT%H:%M:%S"
    ).strftime("%d.%m.%Y %H:%M:%S")
    status = pd_dict["Status"][0]
    log_message = pd_dict["Message"][0]

    # if a query to get the mart update date exists query is performed
    if conn_id and query_path:
        sql = read_query_from_file(query_path)
        updated_dttm = get_mart_update_date(sql, conn_id)
        if updated_dttm == pd.to_datetime("today"):
            updated_dttm_msg = "сегодня"
        elif updated_dttm == datetime(1900, 1, 1, 0, 0):
            updated_dttm_msg = "не удалось получить дату"
        else:
            updated_dttm_msg = updated_dttm.strftime("%d.%m.%Y")
    else:
        updated_dttm_msg = "не запрашивалась"

    # sends data to Telegram
    TOKEN = my_token
    chat_id = my_chat_id

    message = f"""
                Отчет: {report_name},
                дата: {start_time},
                статус: {status},
                дата обновления данных: {updated_dttm_msg},
                сообщение: {log_message}
                """

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()


def get_reports_statuses_for_dept(
    reports_auth: str, reports_dept: str, report_name: str, report_details: str
):
    """Gets report statuses for the specified department"""
    logger.info(f"Get update status for the team {reports_dept}, report {report_name}")

    my_token, my_chat_id, auth = get_dept_details(reports_auth, reports_dept)

    current_report = dict(report_details)
    report_api_refresh_telegram(current_report, my_token, my_chat_id, auth)


# job variables
dag_settings = get_dag_settings(PATH)
reports_auth = os.getenv("reports_authorization")

begin = DummyOperator(task_id="begin", dag=dag)

for reports_dept in dag_settings:
    reports_details = dag_settings[reports_dept]["reports"]
    for rep in reports_details:
        report_name, report_details = list(rep.items())[0]
        report_status_task = PythonOperator(
            task_id=f"report_status_task_{reports_dept}_{report_name}",
            python_callable=get_reports_statuses_for_dept,
            op_kwargs={
                "reports_auth": reports_auth,
                "reports_dept": reports_dept,
                "report_name": report_name,
                "report_details": report_details,
            },
            dag=dag,
        )
        report_status_task.set_upstream(begin)

begin >> report_status_task
