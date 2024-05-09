import pandas as pd 
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task


# Задаем пути до файлов
SRС_PATH = 'files_hw/profit_table.csv'
OUTPUT_PATH = 'files_hw/flags_activity.csv'

PRODUCT_LIST = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# Параметры DAG по умолчанию
default_args = {
    "depends_on_past": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Создаем DAG
@dag('Yaskova_Marina_hw2_parallel', 
    default_args = default_args,
    description = 'DAG для расчета витрины активности клиентов по сумме и количеству их транзакций. Расчет по продуктам параллельный',
    schedule = "0 0 5 * *",
    start_date = datetime(2023, 10, 1),
    catchup = False,
    tags = ['mipt_hw']
)
def profit_flags_extractor_dag():

    @task()
    def extract_and_date_filter(**kwargs):
        # считываем данные из файла-источника
        profit_df = pd.read_csv(SRС_PATH)

        # оставляем только данные за нужные нам даты (фрагмент из скрипта transform)
        date = kwargs['logical_date'] # дата расчёта флагов активности
        
        start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
        end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
        date_list = pd.date_range(
            start=start_date, end=end_date, freq='M'
        ).strftime('%Y-%m-01')
        
        df_tmp = (
            profit_df[profit_df['date'].isin(date_list)]
            .drop('date', axis=1)
            .groupby('id')
            .sum()
        )

        return df_tmp
    

    @task()
    def transform_parallel(df_tmp, product):
        '''
        Собирает таблицу флагов активности для одного продукта
        на основании прибыли и количеству совершёных транзакций
        
        :param df_tmp: таблица с суммой и кол-вом транзакций
        :param product: продукт, для которого считаем флаги активности
        
        :return product_df_tmp: pandas-датафрейм флагов за указанную дату для рассматриваемого продукта
        '''

        df_tmp[f'flag_{product}'] = (
            df_tmp.apply(
                lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0, 
                axis=1
                ).astype(int)
            )

        product_df_tmp = df_tmp.filter(regex='flag').reset_index()
        
        return product_df_tmp # датафрейм с флагами активности для одного продукта


    @task()
    def load(flags_activity_dict):
        '''
        Собирает объединенную таблицу флагов активности и записывает в файл-назначение
        :param flags_activity_dict: словарь с таблицами (датафреймами) флагов активности, собранных для каждого продукта после распараллеливания этапа transform
        '''
        all_flags_activity = pd.DataFrame({'id': flags_activity_dict[PRODUCT_LIST[0]]['id']})

        for product in PRODUCT_LIST: # собираем данные по продуктам, добавляя новые колонки в ранее созданный датафрейм
            col_name = f'flag_{product}'
            all_flags_activity[col_name] = flags_activity_dict[product][col_name]

        all_flags_activity.to_csv(OUTPUT_PATH, index=False, header=False, mode='a')
                
                
    # Пайплайн DAG-а
    profit_df = extract_and_date_filter()

    # Распараллеливаем обработку по продуктам
    flags_activity = {}
    for product in PRODUCT_LIST:
        flags_activity[product] = transform_parallel(profit_df, product)


    load(flags_activity)
                

profit_flags_extractor_dag = profit_flags_extractor_dag() # инициализация DAG
            