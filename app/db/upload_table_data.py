import os
from datetime import timedelta, date

import psycopg2
import pandas as pd
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


def get_pers_num(license: str, act_dt) -> bool | int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""SELECT personnel_num
                    FROM dim_drivers
                    WHERE driver_license_num = '{license}' AND start_dt <= '{act_dt}'
                    ORDER BY personnel_num
                    LIMIT 1""")
        pers_num = cur.fetchone()
    cur.close()
    conn.close()
    # if pers_num is None:
    #     print(f'{license}, start_dt = {act_dt}')
    return 2147483647 if pers_num is None else pers_num[0]

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

def get_all_dim_drivers():
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT * FROM dim_drivers")
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
                cur.execute(update_query, (
                    row['update_dt'] - timedelta(seconds=1),
                    row['plate_num']
                ))
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
                cur.execute(update_query, (
                    row['dt'] - timedelta(seconds=1),
                    row['client_phone']
                ))
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
                    WHERE driver_license_num = '{driver_license_num}'
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
                cur.execute(update_query, (
                    row['update_dt'] - timedelta(seconds=1),
                    row['driver_license_num']
                ))
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


def get_personnel_num(car_plate_num: str, arr_dt) -> int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""
                    SELECT driver_pers_num
                    FROM fact_waybills
                    WHERE car_plate_num='{car_plate_num}' AND work_start_dt <= '{arr_dt}' AND work_end_dt >= '{arr_dt}'
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
            personnel_num = get_personnel_num(
                car_plate_num=row['car_plate_num'],
                arr_dt=arrival_dt
            )
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


def load_fio_drivers_card_amt(days_back: int):
    conn_2  = create_conn()
    cur = conn_2.cursor()
    with cur as cur:
        cur.execute(f"""SELECT fr.driver_pers_num, max(dd.last_name) last_name, 
                            		max(dd.first_name) first_name, 
                            		max(dd.middle_name) middle_name,
                            		max(dd.card_num) card_num, 
                            		round(sum(fr.price_amt - (fr.price_amt*.2 + 47.26*7*fr.distance_val/100 + 5*fr.distance_val)), 2) as amount
                        FROM fact_rides fr left join dim_drivers dd on fr.driver_pers_num = dd.personnel_num
                        WHERE DATE(ride_end_dt) = (CURRENT_DATE - INTEGER '{days_back}')
                        group by fr.driver_pers_num
                        having fr.driver_pers_num != '2147483647'
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['personnel_num', 'last_name', 'first_name',  'middle_name', 'card_num', 'amount'])
    conn_2.close()
    return df


def add_data_to_rep_drivers_payments(df, days_back: int):
    if not df.empty:
        conn_2 = create_conn()
        cur = conn_2.cursor()
        report_dt = date.fromordinal(date.today().toordinal() - days_back +  1)
        for index, row in df.iterrows():
            insert_query = """
                        INSERT INTO  rep_drivers_payments (personnel_num, last_name, first_name,  middle_name, card_num, amount, report_dt)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """
            record_to_insert = (
                row['personnel_num'],
                row['last_name'],
                row['first_name'],
                row['middle_name'],
                row['card_num'],
                row['amount'],
                report_dt,
            )
            cur.execute(insert_query, record_to_insert)
        try:
            conn_2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn_2.rollback()
            cur.close()
        cur.close()
    return


def load_speed_cnt_violations():
    conn_2  = create_conn()
    cur = conn_2.cursor()
    with cur as cur:
        cur.execute(""" SELECT  dd.personnel_num,
                                	fr.ride_id,
                                	max(case when EXTRACT(MINUTE FROM fr.ride_end_dt-fr.ride_start_dt) = 0 then
                                	0 else (fr.distance_val / EXTRACT(MINUTE FROM fr.ride_end_dt-fr.ride_start_dt)) * 60 end) speed,
                                	row_number() over (partition by personnel_num) - 1 violations_cnt
                                	
                        from fact_rides fr left join dim_drivers dd on dd.personnel_num = fr.driver_pers_num
                        where dd.personnel_num is not Null
                        group by dd.personnel_num,fr.ride_id
                                				   
                        having max(case when EXTRACT(MINUTE FROM fr.ride_end_dt-fr.ride_start_dt) = 0
                                        then 0
                                        else (fr.distance_val / EXTRACT(MINUTE FROM fr.ride_end_dt-fr.ride_start_dt))*60 
                                   end) > 85
                                				   
                        order by dd.personnel_num
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['personnel_num', 'ride', 'speed',  'violations_cnt'])
    conn_2.close()
    return df


def add_data_to_rep_drivers_violations(df):
    if not df.empty:
        conn_2 = create_conn()
        cur = conn_2.cursor()
        cur.execute("""DELETE FROM rep_drivers_violations""")
        for index, row in df.iterrows():
            insert_query = """
                        INSERT INTO  rep_drivers_violations (personnel_num, ride, speed, violations_cnt)
                        VALUES (%s,%s,%s,%s)
                        """
            record_to_insert = (
                row['personnel_num'],
                row['ride'],
                row['speed'],
                row['violations_cnt'],
            )
            cur.execute(insert_query, record_to_insert)
        try:
            conn_2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn_2.rollback()
            cur.close()
        cur.close()
    return


def load_data_for_overtime():
    conn_2  = create_conn()
    cur = conn_2.cursor()
    with cur as cur:
        cur.execute(""" with t1 as (
                        select  work_start_dt, work_end_dt, driver_pers_num,
                                lag(work_end_dt) over(partition by driver_pers_num order by work_start_dt, work_end_dt) le
                        from fact_waybills
                                   ),
                             
                        t2 as (
                        select  work_start_dt, work_end_dt, driver_pers_num,
                              case when work_start_dt < max(le) over(order by work_start_dt, work_end_dt) 
                                THEN null else work_start_dt
                                end new_start
                        from t1
                        ),
                        
                        t3 as (select work_start_dt , work_end_dt, driver_pers_num,
                               max(new_start) over(order by work_start_dt, work_end_dt) left_edge
                        from t2
                        ),
                        
                        t4 as (select 
                            case when (EXTRACT (day FROM  work_end_dt) - EXTRACT(day FROM  work_start_dt)) =1 then min(work_end_dt) else max(work_end_dt) end max_dt,
                            min(work_start_dt) min_dt, driver_pers_num
                        from t3
                        
                        group by left_edge, driver_pers_num,work_start_dt,work_end_dt)
                        
                        select driver_pers_num, min(min_dt) work_start_dt, sum(max_dt - min_dt) work_hours
                        from t4
                        group by driver_pers_num, date(min_dt)
                        having sum(max_dt - min_dt) > '07:00:00' and driver_pers_num != '2147483647'
                        order by driver_pers_num, work_start_dt
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['personnel_num', 'start_waybill', 'sum_work_time'])
    conn_2.close()
    return df


def add_data_to_rep_drivers_overtime(df):
    if not df.empty:
        conn_2 = create_conn()
        cur = conn_2.cursor()
        cur.execute("""DELETE FROM rep_drivers_overtime""")
        for index, row in df.iterrows():
            insert_query = """
                        INSERT INTO  rep_drivers_overtime (personnel_num, start_waybill, sum_work_time)
                        VALUES (%s,%s,%s)
                        """
            record_to_insert = (
                row['personnel_num'],
                row['start_waybill'],
                row['sum_work_time'],
            )
            cur.execute(insert_query, record_to_insert)
        try:
            conn_2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn_2.rollback()
            cur.close()
        cur.close()
    return


def load_clients_hist():
    conn_2  = create_conn()
    cur = conn_2.cursor()
    with cur as cur:
        cur.execute(""" with t as   (
                                    select  
                                        	replace(card_num, ' ', '') card_num,
                                        	start_dt, end_dt,
                                        	deleted_flag,
                                        	phone_num
                                    from dim_clients
                                    ),
                            t2 as   (
                                    select 
                                        ride_id, 
                                        	max(price_amt) price_amt,
                                        	max(client_phone_num) phone_num,
                                        	max(deleted_flag) deleted_flag,
                                        	max(start_dt) start_dt,
                                        	max(end_dt) end_dt, 
                                        	max(case when transaction_amt > price_amt then null else transaction_id end) transaction_id, 
                                        	max(case when transaction_amt > price_amt then null else transaction_amt end) transaction_amt,
                                        	max(fr.ride_start_dt) ride_start_dt
                                    from fact_rides fr 
                                    left join t on fr.client_phone_num = t.phone_num
                                    left join fact_payments fp on t.card_num  = fp.card_num
                                    group by ride_id
                                    )
                                
                            select 
                                    phone_num,
                                    count(ride_id) rides_cnt,
                                    sum(case when ride_start_dt is null then 1 else 0 end) cancelled_cnt,
                                    sum(case when transaction_id is null then 0 else transaction_amt end) spent_amt,
                                    sum(case when transaction_id is null then price_amt else 0 end) debt_amt,
                                    min(start_dt) start_dt
                                
                            from t2
                            group by phone_num
                            order by start_dt
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['phone_num', 'rides_cnt', 'cancelled_cnt',  'spent_amt', 'debt_amt', 'start_dt'])
    conn_2.close()
    return df


def add_data_to_rep_clients_hist(df):
    if not df.empty:
        conn_2 = create_conn()
        cur = conn_2.cursor()
        cur.execute("""DELETE FROM rep_clients_hist""")
        for index, row in df.iterrows():
            insert_query = """
                        INSERT INTO  rep_clients_hist (phone_num, rides_cnt, cancelled_cnt, spent_amt, debt_amt, start_dt)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        """
            record_to_insert = (
                row['phone_num'],
                row['rides_cnt'],
                row['cancelled_cnt'],
                row['spent_amt'],
                row['debt_amt'],
                row['start_dt'],
            )
            cur.execute(insert_query, record_to_insert)
        try:
            conn_2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn_2.rollback()
            cur.close()
        cur.close()
    return
