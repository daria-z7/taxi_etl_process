"""Главный скрипт ETL процесса.
Настроено логирование основных событий и ошибок.
1)Выполняется считывание данных из базы-источника и заполнение таблиц базы-хранилища.
2)Выполняется составление 4 отчетов.
"""
import logging

from db.load_table_data import (load_new_driver_records, load_new_car_records,
                                load_new_client_records, load_new_rides_records,)
from db.load_file_data import get_waybills_data, get_payment_data
from db.upload_table_data import (add_data_to_dim_drivers, add_data_to_cars,
                             add_data_to_clients, add_data_to_payments,
                             add_data_to_waybill, add_data_to_fact_rides,
                             add_date_to_check_load, check_load,
                             load_fio_drivers_card_amt, add_data_to_rep_drivers_payments,
                             load_speed_cnt_violations, add_data_to_rep_drivers_violations,
                             load_data_for_overtime, add_data_to_rep_drivers_overtime,
                             load_clients_hist, add_data_to_rep_clients_hist)
from db.functions import yesterday


logging.basicConfig(
    level=logging.INFO,
    filename='main.log',
    filemode='a',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s'
)


def load_data(days_back: int):
    """Основная логика работы скрипта."""
    yesterday_dt = yesterday(days_back=days_back)
    print(yesterday_dt)
    if not check_load(date_dt=yesterday_dt):
        logging.info(f'Данные за {yesterday_dt} уже загружены.')
        return

    try:
        """Загрузка данных по водителям."""
        df_drivers = load_new_driver_records(days_back=days_back)
        add_data_to_dim_drivers(df_drivers)
        logging.info(f'Данные dim_drivers загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по водителям: {error}')
        return

    try:
        """Загрузка данных по машинам."""
        df_cars = load_new_car_records(days_back=days_back)
        add_data_to_cars(df_cars)
        logging.info(f'Данные dim_cars загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по машинам: {error}')
        return

    try:
        """Загрузка данных по клиентам."""
        df_clients = load_new_client_records(days_back=days_back)
        add_data_to_clients(df_clients)
        logging.info(f'Данные dim_clients загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по клиентам: {error}')
        return
    
    try:
        """Загрузка данных по путевым листам."""
        df_waybills = get_waybills_data(days_back=days_back)
        table = 'fact_waybills'
        add_data_to_waybill(df=df_waybills, table=table)
        logging.info(f'Данные fact_waybills загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по путевым листам: {error}')
        return

    try:
        """Загрузка данных по платежам."""
        df_payments = get_payment_data(days_back=days_back)
        table = 'fact_payments'
        add_data_to_payments(df=df_payments, table=table)
        logging.info(f'Данные fact_payments загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по платежам: {error}')
        return

    try:
        """Загрузка данных по поездкам."""
        df_rides = load_new_rides_records(days_back=days_back)
        add_data_to_fact_rides(df=df_rides)
        logging.info(f'Данные fact_rides загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при загрузке данных по поездкам: {error}')
        return
    
    try:
        add_date_to_check_load(yesterday_dt)
    except Exception as error:
        logging.error(f'Ошибка при записи данных в work_load_info: {error}')
        return
    
    try:
        """Ежедневная витрина - выплата водителям."""
        df_payments = load_fio_drivers_card_amt(days_back=days_back)
        add_data_to_rep_drivers_payments(df=df_payments, days_back=days_back)
        logging.info(f'Данные rep_drivers_payments загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при составлении отчета rep_drivers_payments: {error}')
        return
    
    try:
        """Ежедневная витрина - водители нарушители."""
        df_violations = load_speed_cnt_violations()
        add_data_to_rep_drivers_violations(df=df_violations)
        logging.info(f'Данные rep_drivers_violations загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при составлении отчета rep_drivers_violations: {error}')
        return
    
    try:
        """Ежедневная витрина - перерабатывающие водители."""
        df_overtime = load_data_for_overtime()
        add_data_to_rep_drivers_overtime(df=df_overtime)
        logging.info(f'Данные rep_drivers_overtime загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при составлении отчета rep_drivers_overtime: {error}')
        return
    
    try:
        """Историчная витрина - знай своего клиента."""
        df_clients_hist = load_clients_hist()
        add_data_to_rep_clients_hist(df=df_clients_hist)
        logging.info(f'Данные rep_clients_hist загружены ({yesterday_dt}).')
    except Exception as error:
        logging.error(f'Ошибка при составлении отчета rep_clients_hist: {error}')
        return

    return


if __name__ == "__main__":
    days_back = 1
    load_data(days_back=days_back)
