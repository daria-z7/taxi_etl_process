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

def load_new_driver_records():
    conn  = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute("SELECT * FROM drivers WHERE DATE(update_dt) = (CURRENT_DATE - INTEGER '1')")
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['driver_license_num', 'first_name', 'last_name',  'middle_name', 'driver_valid_to', 'card_num', 'update_dt', 'birth_dt'])
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

def check_drivers_record_exists(driver_license_num) -> bool:
    conn = create_conn_2()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT card_num FROM dim_drivers WHERE driver_license_num LIKE '{driver_license_num}' ORDER BY personnel_num DESC LIMIT 1")
        driver_card = cur.fetchone()
        
    cur.close()
    conn.close()
    return False if driver_card is None else driver_card[0]

def add_data_to_dim_drivers(df: list):
    if not df.empty:
        conn = create_conn_2()
        cur = conn.cursor()
        for index, row in df.iterrows():
            # print(row['finished_flag'])
            db_card = check_drivers_record_exists(row['driver_license_num'])
            if not db_card:
                print('good')
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
                print("!")
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


if __name__ == "__main__":
    # df = load_new_car_records()
    # print(df)
    # add_data_to_cars(df=df)
    # get_all_cars()
    df = load_new_driver_records()
    print(df)
    add_data_to_dim_drivers(df)