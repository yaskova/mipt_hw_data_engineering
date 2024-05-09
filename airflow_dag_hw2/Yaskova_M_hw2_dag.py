import pandas as pd 
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task


# Задаем пути до файлов
SRС_PATH = 'files_hw/profit_table.csv'
OUTPUT_PATH = 'files_hw/flags_activity.csv'


# Параметры DAG по умолчанию
default_args = {
    "depends_on_past": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Создаем DAG
@dag('Yaskova_Marina_hw2', 
    default_args = default_args,
    description = 'DAG для расчета витрины активности клиентов по сумме и количеству их транзакций',
    schedule = "0 0 5 * *",
    start_date = datetime(2023, 10, 1),
    catchup = False,
    tags = ['mipt_hw']
)
def profit_flags_extractor_dag():

    @task()
    def extract():
        # считываем данные из файла-источника
        profit_df = pd.read_csv(SRС_PATH)
        return profit_df
    
    @task()
    def transform(profit_table, **kwargs): # код выданного в задании скрипта, но вместо date достанем logical_date
        '''
        Собирает таблицу флагов активности по продуктам
        на основании прибыли и количеству совершёных транзакций
        
        :param profit_table: таблица с суммой и кол-вом транзакций
        
        :return df_tmp: pandas-датафрейм флагов за указанную дату
        '''
        date = kwargs['logical_date'] # дата расчёта флагов активности
        
        start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
        end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
        date_list = pd.date_range(
            start=start_date, end=end_date, freq='M'
        ).strftime('%Y-%m-01')
        
        df_tmp = (
            profit_table[profit_table['date'].isin(date_list)]
            .drop('date', axis=1)
            .groupby('id')
            .sum()
        )
        
        product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        for product in product_list:
            df_tmp[f'flag_{product}'] = (
                df_tmp.apply(
                    lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
                    axis=1
                ).astype(int)
            )
            
        df_tmp = df_tmp.filter(regex='flag').reset_index()
        
        return df_tmp #датафрейм с флагами активности

    @task()
    def load(flags_activity):
        '''
        Записывает таблицу флагов активности в файл-назначение
        :param flags_activity: таблица флагов активности, датафрейм. Сюда подаем df_tmp из задачи transform
        '''
        flags_activity.to_csv(OUTPUT_PATH, index=False, header=False, mode='a')
                
                
    # Пайплайн DAG-а
    profit_df = extract()
    flags_activity = transform(profit_df)
    load(flags_activity)
                

profit_flags_extractor_dag = profit_flags_extractor_dag() # инициализация DAG
            