import psycopg2
import pandas as pd

from db_functions import add_data_to_cars, get_all_cars, add_data_to_clients, get_all_clients


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
    return df

def create_conn_2():
    shema = 'dwh_saint_petersburg'
    conn_2 = psycopg2.connect(host='de-edu-db.chronosavant.ru',
                            port=5432,
                            database='dwh',
                            user='dwh_saint_petersburg',
                            password='dwh_saint_petersburg_6na1uFWY',
                            options=f'-c search_path={shema}',
                        )
    return conn_2

def check_car_record_exists(plate_num) -> bool:
    conn = create_conn_2()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT plate_num FROM dim_cars WHERE plate_num LIKE '{plate_num}' ORDER BY revision_dt DESC LIMIT 1")
        trans_id = cur.fetchone()
        # print(trans_id[0])
    cur.close()
    conn.close()
    return False if trans_id is None else True

def add_data_to_dim_drivers(df: list):
    if not df.empty:
        conn = create_conn_2()
        cur = conn.cursor()
        for index, row in df.iterrows():
            # print(row['finished_flag'])
            if not check_car_record_exists(row['plate_num']):
                print('hi')
                insert_query = """
                            INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
                    None,
                )
                cur.execute(insert_query, record_to_insert)
            else:
                print("!")
                update_query = """
                               UPDATE  dim_cars
                               SET end_dt = %s
                               WHERE plate_num = %s
                               ORDER BY revision_dt DESC
                               LIMIT 1
                               """
                cur.execute(update_query, (row['update_dt'], row['plate_num']))
                insert_query = """
                               INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt)
                               VALUES (%s,%s,%s,%s,%s,%s)
                               """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
                    None,
                )
                cur.execute(insert_query, record_to_insert)
        try:
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cur.close()
        cur.close()
    return
