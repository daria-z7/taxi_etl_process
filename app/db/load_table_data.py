import os

import psycopg2
import pandas as pd
from dotenv import load_dotenv


load_dotenv('.env')

def create_conn() -> str:
    shema = os.getenv('SOURCE_DB_SHEMA')
    conn = psycopg2.connect(
        host=os.getenv('SOURCE_DB_HOST'),
        port=os.getenv('SOURCE_DB_PORT'),
        database=os.getenv('SOURCE_DB_NAME'),
        user=os.getenv('SOURCE_DB_USER'),
        password=os.getenv('SOURCE_DB_PASSWORD'),
        options=f'-c search_path={shema}',
    )
    return conn


def load_new_car_records(days_back: int):
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT *
                    FROM car_pool
                    WHERE DATE(update_dt) = (CURRENT_DATE - INTEGER '{days_back}')
                    """
                )
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= [
        'plate_num',
        'model',
        'revision_dt',
        'register_dt',
        'finished_flag',
        'update_dt'
    ])
    conn.close()
    df['revision_dt'] = df['revision_dt'].astype('datetime64[ns]')
    df['register_dt'] = df['register_dt'].astype('datetime64[ns]')
    df['update_dt'] = df['update_dt'].astype('datetime64[ns]')
    return df


def load_new_client_records(days_back: int):
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT *
                    FROM rides
                    WHERE DATE(dt) = (CURRENT_DATE - INTEGER '{days_back}')
                    """
                )
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
    conn.close()
    df['dt'] = df['dt'].astype('datetime64[ns]')
    return df


def load_new_driver_records(days_back: int):
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT *
                    FROM drivers
                    WHERE DATE(update_dt) = (CURRENT_DATE - INTEGER '{days_back}')
                    """
                )
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= [
        'driver_license_num',
        'first_name',
        'last_name', 
        'middle_name',
        'driver_valid_to',
        'card_num',
        'update_dt',
        'birth_dt'
    ])
    conn.close()
    return df


def load_new_rides_records(days_back: int):
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""SELECT r.ride_id, r.dt r_dt, r.client_phone, r.card_num, r.point_from, r.point_to, r.distance, r.price, m.car_plate_num, m.event, m.dt m_dt
                    FROM rides r left join movement m on r.ride_id=m.ride
                    WHERE event in ('CANCEL', 'END') AND DATE(r.dt) = (CURRENT_DATE - INTEGER '{days_back}')""")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= [
        'ride_id',
        'r_dt',
        'client_phone',
        'card_num',
        'point_from',
        'point_to',
        'distance',
        'price',
        'car_plate_num',
        'event',
        'm_dt'
    ])
    conn.close()
    return df


def get_arrival_dt(ride_id, event):
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT dt
                    FROM movement
                    WHERE ride = '{ride_id}' AND event = '{event}'
                    """
                )
        arrival_dt = cur.fetchone()
    cur.close()
    conn.close()
    return arrival_dt[0] if arrival_dt is not None else arrival_dt
