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

def load_speed_cnt_violations():
    conn_2  = create_conn_2()
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
    #cur.close()
    conn_2.close()
    #print(type(df))
    return df


def add_data_to_rep_drivers_violations(df: list):
    if not df.empty:
        conn_2 = create_conn_2()
        cur = conn_2.cursor()
        for index, row in df.iterrows():
            # arrival_dt = get_arrival_dt(row['ride_id'], 'READY')
            # start_dt = get_arrival_dt(row['ride_id'], 'BEGIN')
            # personnel_num = get_personnel_num(row['car_plate_num'])
            # print('good')
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


if __name__ == "__main__":
    df = load_speed_cnt_violations()
    print(df)
    add_data_to_rep_drivers_violations(df)
    print('Successuful')