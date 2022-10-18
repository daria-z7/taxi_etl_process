import psycopg2
import pandas as pd

# from db_functions import add_data_to_drivers, get_all_drivers, add_data_to_drivers, get_all_drivers


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

def load_new_driver_records():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("SELECT * FROM drivers WHERE DATE(update_dt) = (CURRENT_DATE - INTEGER '1')")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['last_name', 'first_name', 'middle_name', 'birth_dt', 'card_num', 'driver_license', 'driver_valid_to'])
    #cur.close()
    conn.close()
    #print(type(df))
    return print(df)


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

def check_drivers_record_exists(personnel_num) -> bool:
    conn = create_conn_2()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT personnel_num FROM dim_drivers WHERE personnel_num LIKE '{personnel_num}' ORDER BY revision_dt DESC LIMIT 1")
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
            if not check_drivers_record_exists(row['personnel_num']):
                print('hi')
                insert_query = """
                            INSERT INTO  dim_drivers (last_name, first_name, middle_name, birth_dt, card_num, driver_license, driver_license_dt)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['last_name'],
                    row['first_name'],
                    row['middle_name'],
                    row['birth_dt'],
                    row['card_num'],
                    row['driver_license'],
                    row['driver_license_dt']
                )
                cur.execute(insert_query, record_to_insert)
            else:
                print("!")
                update_query = """
                               UPDATE  dim_drivers
                               SET end_dt = %s
                               WHERE personnel_num = %s
                               ORDER BY driver_license_dt DESC
                               LIMIT 1
                               """
                cur.execute(update_query, (row['start_dt'], row['personnel_num']))
                insert_query = """
                               INSERT INTO  dim_drivers (last_name, first_name, middle_name, birth_dt, card_num, driver_license, driver_license_dt)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)
                               """
                record_to_insert = (
                    row['last_name'],
                    row['first_name'],
                    row['middle_name'],
                    row['birth_dt'],
                    row['card_num'],
                    row['driver_license'],
                    row['driver_license_dt']
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
