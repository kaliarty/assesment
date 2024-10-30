from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.hooks.base_hook import BaseHook
# from pendulum import datetime, now
from datetime import datetime

import pandas as pd
import logging
import json

default_args = {
    'fs_conn_id': 'fs_conn' # or put here your path
}

def calculate_days_since_last_loan(group):
    # Фильтруем только те займы, где сумма не null
    loans = group[group['summa'].notnull()]

    if loans.empty:
        return -1  # Нет займов

    # Берем последнюю ненулевую дату займа
    last_loan_date = loans[loans['contract_date'].notnull()]['contract_date'].max()

    if pd.isna(last_loan_date):
        return -1  # Нет валидных займов

    # Убедимся, что даты имеют одинаковую временную зону
    application_date = group['application_date'].max()

    # Приведение к одной временной зоне, если необходимо
    if last_loan_date.tzinfo is not None and application_date.tzinfo is None:
        application_date = application_date.tz_localize('UTC')  # Пример локализации в UTC
    elif last_loan_date.tzinfo is None and application_date.tzinfo is not None:
        last_loan_date = last_loan_date.tz_localize('UTC')  # Пример локализации в UTC

    # Вычисляем разницу в днях
    days_since_last_loan = (application_date - last_loan_date).days

    return days_since_last_loan


def transform_data():

    df = pd.read_csv('/opt/airflow/dags/data.csv')
    logging.info('Data was readen from csv file')
    logging.info(df.info()) # best practice is stored this info and check what gain

    # Part dqc
    required_columns = ['id', 'application_date', 'contract']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"Missing column: {col}")

    # part dqc
    try:
        df['application_date'] = pd.to_datetime(df['application_date'])
    except ValueError:
        logging.error("Incorrect date format in application_date")

    for index, row in df.iterrows():
        if pd.isna(row['id']) or pd.isna(row['application_date']):
            print(f"Empty value in a row {index}: id or application_date")

    duplicates = df[df['id'].duplicated(keep=False)]

    if not duplicates.empty:
        logging.error(f"Found duplicate id: {duplicates}")
    duplicates.iloc[0:0]

    for index, row in df.iterrows():
        if pd.isna(row['id']) or pd.isna(row['application_date']):
            logging.error(f"Empty value in a row {index}: id or application_date")

    try:
        df['application_date'] = pd.to_datetime(df['application_date'])
    except ValueError:
        logging.error("Invalid date format in application_date")

    df['id'] = df['id'].astype(int)
    data = []

    for index, row in df.iterrows():
        # id,application_date are remembered for json's array
        id_value = row['id']
        application_date = row['application_date']
        contracts = row['contracts']

        if pd.isna(contracts) or contracts.strip() == '':
            data.append({
                'id': id_value,
                'application_date': pd.to_datetime(application_date),
                'contract_id': '',
                'bank': '',
                'summa': '',
                'loan_summa': '',
                'claim_date': pd.NaT,
                'claim_id': '',
                'contract_date': pd.NaT
            })
        else:
            # need open json's array
            try:
                contracts_list = json.loads(contracts)
                if isinstance(contracts_list, dict):
                    contracts_list = [contracts_list]

                for contract in contracts_list:
                    data.append({
                        'id': id_value,
                        'application_date': pd.to_datetime(application_date),
                        'contract_id': contract.get('contract_id', ''),
                        'bank': contract.get('bank', ''),
                        'summa': contract.get('summa', ''),
                        'loan_summa': contract.get('loan_summa', ''),
                        'claim_date': pd.to_datetime(contract.get('claim_date', ''), errors='coerce'),
                        'claim_id': contract.get('claim_id', ''),
                        'contract_date': pd.to_datetime(contract.get('contract_date', ''), errors='coerce')
                    })
            except json.JSONDecodeError:
                logging.error(f"SYNTAX ERROR IN JSON {index + 1}")

    logging.info('Data was parsed')

    new_df = pd.DataFrame(data)

    new_df.to_csv('/opt/airflow/dags/output_file.csv', index=False)

    newnew_df = new_df.groupby('contract_id').apply(calculate_days_since_last_loan).reset_index(drop=True)
    newnew_df.to_csv('/opt/airflow/dags/newnew_df.csv', index=False)



# Create Simple DAG
with DAG( dag_id= 'parse',
        schedule= '@daily',
        catchup= False,
        start_date= datetime(2024,10,1),
        max_active_runs= 1,
        default_args=default_args) as dag:
    #
    waiting_for_file= FileSensor(
                                task_id= 'waiting_for_file',
                                poke_interval=20,
                                timeout=300,
                                filepath= 'data.csv'
                                )
    transform_data = PythonOperator(
                                task_id='read_csv_task',
                                python_callable=transform_data
                                )
    end = EmptyOperator(task_id = 'end')
    # Set Dependencies Flow
    waiting_for_file >> transform_data >> end


