import psycopg2
import pandas as pd
from datetime import date as dt


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


def add_date_to_check_load(date_dt):
    conn = create_conn_2()
    cur = conn.cursor()
    insert_query = """
                INSERT INTO  work_load_check (date_load)
                VALUES (%s)
                """
    cur.execute(insert_query, (date_dt))
    try:
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
    cur.close()


def check_load(date_dt):
    conn = create_conn_2()
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
    elif last_day_load[0] in date_dt:
        return print(False)
    else:
        return True
    
if __name__ == "__main__":
    date_dt = [dt.today()]
    print(date_dt)
    add_date_to_check_load(date_dt)
    check_load(date_dt)
    print('Successuful')