#import os

import psycopg2
import pandas as pd
from dotenv import load_dotenv

#from db.load_table_data import get_arrival_dt


load_dotenv('.env')

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


def load_cancelled_ride():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("""SELECT ride
                    FROM movement
                    WHERE event in ('CANCEL')""")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['ride_id'])
    #cur.close()
    conn.close()
    #print(type(df))
    return df


def add_data_to_work_cancelled_ride(df: list):
    if not df.empty:
        conn = create_conn_2()
        cur = conn.cursor()
        for index, row in df.iterrows():
            insert_query = """
                        INSERT INTO  work_cancelled_ride (ride_id)
                        VALUES (%s)
                        """
            record_to_insert = (int(row['ride_id']), )
            cur.execute(insert_query, record_to_insert)

        try:
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cur.close()
        cur.close()
    return

if __name__ == "__main__":
    df = load_cancelled_ride()
    print(df)
    add_data_to_work_cancelled_ride(df)
    print('Successuful')