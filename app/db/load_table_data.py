import psycopg2
import pandas as pd

from db_functions import add_data_to_cars, get_all_cars, add_data_to_clients, get_all_clients, delete_all_clients


def create_conn() -> str:
    shema = 'main'

    conn = psycopg2.connect(
        host='de-edu-db.chronosavant.ru',
        port=5432,
        database='taxi',
        user='etl_tech_user',
        password='etl_tech_user_password',
        options=f'-c search_path={shema}',
    )
    return conn


def load_new_car_records():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("SELECT * FROM car_pool WHERE DATE(update_dt) = (CURRENT_DATE - INTEGER '1')")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['plate_num', 'model', 'revision_dt', 'register_dt', 'finished_flag', 'update_dt'])
    #cur.close()
    conn.close()
    #print(type(df))
    df['revision_dt'] = df['revision_dt'].astype('datetime64[ns]')
    df['register_dt'] = df['register_dt'].astype('datetime64[ns]')
    df['update_dt'] = df['update_dt'].astype('datetime64[ns]')
    return df


def load_new_client_records():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("SELECT * FROM rides WHERE DATE(dt) = (CURRENT_DATE - INTEGER '1')")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= [
        'ride_id',
        'dt',
        'client_phone',
        'card_num',
        'point_from',
        'point_to',
        'distance',
        'price',
    ])
    #cur.close()
    conn.close()
    #print(type(df))
    df['dt'] = df['dt'].astype('datetime64[ns]')
    return df


if __name__ == "__main__":
    df = load_new_car_records()
    print(df)
    #add_data_to_cars(df=df)
    # get_all_cars()
    #df = load_new_client_records()
    # print(df)
    #add_data_to_clients(df=df)
    #get_all_clients()
    # delete_all_clients()
