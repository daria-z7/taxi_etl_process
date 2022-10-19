import os

import psycopg2
from dotenv import load_dotenv

from db.load_table_data import get_arrival_dt


load_dotenv('.env')

def create_conn():
    shema = os.getenv('WH_DB_SHEMA')
    conn = psycopg2.connect(
        host=os.getenv('WH_DB_HOST'),
        port=os.getenv('WH_DB_PORT'),
        database=os.getenv('WH_DB_NAME'),
        user=os.getenv('WH_DB_USER'),
        password=os.getenv('WH_DB_PASSWORD'),
        options=f'-c search_path={shema}',
    )
    return conn


def get_pers_num(license: str) -> bool | int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""SELECT personnel_num
                    FROM dim_drivers
                    WHERE driver_license_num LIKE '{license}'
                    ORDER BY personnel_num DESC
                    LIMIT 1""")
        pers_num = cur.fetchone()
    cur.close()
    conn.close()
    return False if pers_num is None else pers_num[0]

def get_last_trans_id() -> int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT transaction_id
                    FROM fact_payments
                    ORDER BY transaction_id DESC
                    LIMIT 1
                    """
                )
        trans_id = cur.fetchone()
    cur.close()
    conn.close()
    return 0 if trans_id is None else trans_id[0]


def check_car_record_exists(plate_num) -> bool:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT plate_num
                    FROM dim_cars
                    WHERE plate_num LIKE '{plate_num}'
                    ORDER BY revision_dt DESC
                    LIMIT 1
                    """
                )
        trans_id = cur.fetchone()
    cur.close()
    conn.close()
    return False if trans_id is None else True


def get_all_cars():
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT * FROM dim_cars")
        print(cur.fetchall())
    cur.close()
    conn.close()

def get_all_clients():
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT * FROM dim_clients")
        print(cur.fetchall())
    cur.close()
    conn.close()

def get_all_waybills():
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT * FROM fact_waybills")
        print(cur.fetchall())
    cur.close()
    conn.close()

def delete_all_clients():
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"DELETE FROM dim_clients")
        conn.commit()
    cur.close()
    conn.close()


def check_client_card_changed(client_phone: str) -> bool | str:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT phone_num, card_num FROM dim_clients WHERE phone_num LIKE '{client_phone.strip()}' ORDER BY start_dt DESC LIMIT 1")
        client_record = cur.fetchone()
    cur.close()
    conn.close()
    return False if client_record is None else client_record[1]


def add_data_to_payments(df: list, table: str):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query = "INSERT INTO %s(%s) VALUES (%%s, %%s, %%s, %%s)" % (table, cols)
        try:
            cur.executemany(query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cur.close()
        cur.close()
    return


def add_data_to_waybill(df: list, table: str):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query = "INSERT INTO %s(%s) VALUES (%%s, %%s, %%s, %%s, %%s, %%s)" % (table, cols)
        try:
            cur.executemany(query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cur.close()
        cur.close()
    return


def add_data_to_cars(df: list):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        for index, row in df.iterrows():
            if not check_car_record_exists(row['plate_num']):
                insert_query = """
                            INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag)
                            VALUES (%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
                )
                cur.execute(insert_query, record_to_insert)
            else:
                update_query = """
                               UPDATE  dim_cars
                               SET end_dt = %s
                               WHERE plate_num = %s AND end_dt is NULL
                               """
                cur.execute(update_query, (row['update_dt'], row['plate_num']))
                insert_query = """
                               INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag)
                               VALUES (%s,%s,%s,%s,%s)
                               """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
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


def add_data_to_clients(df: list):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        for index, row in df.iterrows():
            db_card = check_client_card_changed(row['client_phone'])
            if not db_card:
                insert_query = """
                            INSERT INTO  dim_clients (phone_num, start_dt, card_num, deleted_flag)
                            VALUES (%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['client_phone'].strip(),
                    row['dt'],
                    row['card_num'].strip(),
                    0,
                )
                cur.execute(insert_query, record_to_insert)
                try:
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    conn.rollback()
                    cur.close()
                    return
            elif row['card_num'].strip() != db_card.strip():
                update_query = """
                               UPDATE  dim_clients
                               SET end_dt = %s
                               WHERE phone_num = %s AND end_dt is NULL
                               """
                cur.execute(update_query, (row['dt'], row['client_phone']))
                insert_query = """
                            INSERT INTO  dim_clients (phone_num, start_dt, card_num, deleted_flag)
                            VALUES (%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['client_phone'].strip(),
                    row['dt'],
                    row['card_num'].strip(),
                    0,
                )
                cur.execute(insert_query, record_to_insert)
                try:
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    conn.rollback()
                    cur.close()
                    return
        cur.close()
    return


def check_drivers_record_exists(driver_license_num) -> bool:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT card_num
                    FROM dim_drivers
                    WHERE driver_license_num LIKE '{driver_license_num}'
                    ORDER BY personnel_num DESC
                    LIMIT 1
                    """
                )
        driver_card = cur.fetchone()   
    cur.close()
    conn.close()
    return False if driver_card is None else driver_card[0]


def add_data_to_dim_drivers(df: list):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        for index, row in df.iterrows():
            db_card = check_drivers_record_exists(row['driver_license_num'])
            if not db_card:
                insert_query = """
                            INSERT INTO  dim_drivers (start_dt, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['update_dt'],
                    row['last_name'].strip(),
                    row['first_name'].strip(),
                    row['middle_name'].strip(),
                    row['birth_dt'],
                    row['card_num'].strip(),
                    row['driver_license_num'].strip(),
                    row['driver_valid_to']
                )
                cur.execute(insert_query, record_to_insert)
            elif db_card != row['card_num']:
                update_query = """
                               UPDATE  dim_drivers
                               SET end_dt = %s
                               WHERE driver_license_num = %s AND end_dt is Null
                               """
                cur.execute(update_query, (row['update_dt'], row['driver_license_num']))
                insert_query = """
                               INSERT INTO  dim_drivers (start_dt, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                               """
                record_to_insert = (
                    row['update_dt'],
                    row['last_name'].strip(),
                    row['first_name'].strip(),
                    row['middle_name'].strip(),
                    row['birth_dt'],
                    row['card_num'].strip(),
                    row['driver_license_num'].strip(),
                    row['driver_valid_to']
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


def get_personnel_num(car_plate_num: str) -> int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT driver_pers_num
                    FROM fact_waybills
                    WHERE car_plate_num='{car_plate_num}'
                    """
                )
        personnel_num = cur.fetchone()
    cur.close()
    conn.close()
    return personnel_num[0]


def add_data_to_fact_rides(df: list):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        for index, row in df.iterrows():
            arrival_dt = get_arrival_dt(row['ride_id'], 'READY')
            start_dt = get_arrival_dt(row['ride_id'], 'BEGIN')
            personnel_num = get_personnel_num(row['car_plate_num'])
            insert_query = """
                        INSERT INTO  fact_rides (ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
            record_to_insert = (
                row['ride_id'],
                row['point_from'].strip(),
                row['point_to'].strip(),
                row['distance'],
                row['price'],
                row['client_phone'].strip(),
                personnel_num,
                row['car_plate_num'].strip(),
                arrival_dt, 
                start_dt,
                row['m_dt']
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


def add_date_to_check_load(date_dt):
    conn = create_conn()
    cur = conn.cursor()
    insert_query = """
                INSERT INTO  work_load_check (date_load)
                VALUES (%s)
                """
    cur.execute(insert_query, (date_dt,))
    try:
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
    cur.close()


def check_load(date_dt):
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("""SELECT date_load
                    FROM work_load_check
                    order by id_load desc
                    limit 1;""")
        last_day_load = cur.fetchone()
    cur.close()
    conn.close()
    
    if last_day_load is None:
        return True
    elif str(last_day_load[0]) == str(date_dt):
        return False
    else:
        return True
