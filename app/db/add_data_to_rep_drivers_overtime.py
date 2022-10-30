import psycopg2
import pandas as pd
from datetime import date

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

def load_data_for_overtime():
    conn_2  = create_conn_2()
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
                        
                        t4 as (select max(work_end_dt) max_dt, min(work_start_dt) min_dt, driver_pers_num
                        from t3
                        
                        group by left_edge, driver_pers_num )
                        
                        select driver_pers_num, min(min_dt) work_start_dt, sum(max_dt - min_dt) work_hours
                        from t4
                        group by driver_pers_num
                        having sum(max_dt - min_dt) > '07:00:00' and driver_pers_num != '2147483647'
                        order by driver_pers_num, work_start_dt
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['personnel_num', 'start_waybill', 'sum_work_time'])
    #cur.close()
    conn_2.close()
    #print(type(df))
    return df


def add_data_to_rep_drivers_overtime(df: list):
    if not df.empty:
        conn_2 = create_conn_2()
        cur = conn_2.cursor()
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


if __name__ == "__main__":
    # df = load_new_car_records()
    # print(df)
    # add_data_to_cars(df=df)
    # get_all_cars()
    df = load_data_for_overtime()
    print(df)
    add_data_to_rep_drivers_overtime(df)
    print('Successuful')