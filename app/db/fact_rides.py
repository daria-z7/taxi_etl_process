import psycopg2
import pandas as pd


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

def load_new_rides_records():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("""SELECT r.ride_id, r.dt r_dt, r.client_phone, r.card_num, r.point_from, r.point_to, r.distance, r.price, m.car_plate_num, m.event, m.dt m_dt
                    FROM rides r left join movement m on r.ride_id=m.ride
                    WHERE event in ('CANCEL', 'END') AND DATE(r.dt) = (CURRENT_DATE - INTEGER '5')""")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['ride_id', 'r_dt', 'client_phone',  'card_num', 'point_from', 'point_to', 'distance', 'price', 'car_plate_num', 'event', 'm_dt'])
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

def get_arrival_dt(ride_id, event):
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""SELECT dt FROM movement WHERE ride = '{ride_id}' AND event = '{event}'""")
        arrival_dt = cur.fetchone()
    cur.close()
    conn.close()
    return arrival_dt[0] if arrival_dt is not None else arrival_dt

def get_personnel_num(car_plate_num):
    conn = create_conn_2()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"""SELECT driver_pers_num FROM fact_waybills WHERE car_plate_num='{car_plate_num}'""")
        personnel_num = cur.fetchone()
    cur.close()
    conn.close()
    
    return personnel_num[0]


def add_data_to_fact_rides(df: list):
    if not df.empty:
        conn = create_conn_2()
        cur = conn.cursor()
        for index, row in df.iterrows():
            arrival_dt = get_arrival_dt(row['ride_id'], 'READY')
            start_dt = get_arrival_dt(row['ride_id'], 'BEGIN')
            personnel_num = get_personnel_num(row['car_plate_num'])
            # print('good')
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


if __name__ == "__main__":
    # df = load_new_car_records()
    # print(df)
    # add_data_to_cars(df=df)
    # get_all_cars()
    df = load_new_rides_records()
    print(df)
    add_data_to_fact_rides(df)
    print('Successuful')