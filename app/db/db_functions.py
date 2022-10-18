import psycopg2


def create_conn():
    shema = 'dwh_saint_petersburg'
    conn = psycopg2.connect(host='de-edu-db.chronosavant.ru',
                            port=5432,
                            database='dwh',
                            user='dwh_saint_petersburg',
                            password='dwh_saint_petersburg_6na1uFWY',
                            options=f'-c search_path={shema}',
                        )
    return conn


def get_pers_num(license: str) -> int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT personnel_num FROM dim_drivers WHERE driver_license_num LIKE '{license}' ORDER BY personnel_num DESC LIMIT 1")
        pers_num = cur.fetchone()
        # print(cur.fetchone())
    cur.close()
    conn.close()
    return 0 if pers_num is None else pers_num[0]

def get_last_trans_id() -> int:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT transaction_id FROM fact_payments ORDER BY transaction_id DESC LIMIT 1")
        trans_id = cur.fetchone()
    cur.close()
    conn.close()
    return 0 if trans_id is None else trans_id[0]


def check_car_record_exists(plate_num) -> bool:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT plate_num FROM dim_cars WHERE plate_num LIKE '{plate_num}' ORDER BY revision_dt DESC LIMIT 1")
        trans_id = cur.fetchone()
        # print(trans_id[0])
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


def check_client_card_changed(client_phone: str) -> bool | str:
    conn = create_conn()
    cur = conn.cursor()
    with cur as cur:
        cur.execute(f"SELECT phone_num, card_num FROM dim_clients WHERE phone_num LIKE '{client_phone.strip()}' ORDER BY start_dt DESC LIMIT 1")
        client_record = cur.fetchone()
        # print(trans_id[0])
    cur.close()
    conn.close()
    return False if client_record is None else client_record[1]


def add_data_to_payments(df: list, table: str):
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


def add_data_to_cars(df: list):
    if not df.empty:
        conn = create_conn()
        cur = conn.cursor()
        for index, row in df.iterrows():
            # print(row['finished_flag'])
            if not check_car_record_exists(row['plate_num']):
                print('hi')
                insert_query = """
                            INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
                    None,
                )
                cur.execute(insert_query, record_to_insert)
            else:
                print("!")
                update_query = """
                               UPDATE  dim_cars
                               SET end_dt = %s
                               WHERE plate_num = %s
                               ORDER BY revision_dt DESC
                               LIMIT 1
                               """
                cur.execute(update_query, (row['update_dt'], row['plate_num']))
                insert_query = """
                               INSERT INTO  dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt)
                               VALUES (%s,%s,%s,%s,%s,%s)
                               """
                record_to_insert = (
                    row['plate_num'],
                    row['update_dt'],
                    row['model'].strip(),
                    row['revision_dt'],
                    row['finished_flag'],
                    None,
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
            # print(row['finished_flag'])
            db_card = check_client_card_changed(row['client_phone'])
            if not db_card:
                # print('hi')
                insert_query = """
                            INSERT INTO  dim_clients (phone_num, start_dt, card_num, deleted_flag, end_dt)
                            VALUES (%s,%s,%s,%s,%s)
                            """
                record_to_insert = (
                    row['client_phone'].strip(),
                    row['dt'],
                    row['card_num'].strip(),
                    'N',
                    None,
                )
                cur.execute(insert_query, record_to_insert)
            elif row['card_num'] != db_card:
                print("!")
                # update_query = """
                #                UPDATE  dim_clients
                #                SET end_dt = %s
                #                WHERE phone_num = %s
                #                ORDER BY start_dt DESC
                #                LIMIT 1
                #                """
                # cur.execute(update_query, (row['dt'], row['client_phone']))
                # insert_query = """
                #             INSERT INTO  dim_clients (phone_num, start_dt, card_num, deleted_flag, end_dt)
                #             VALUES (%s,%s,%s,%s,%s)
                #             """
                # record_to_insert = (
                #     row['client_phone'].strip(),
                #     row['dt'],
                #     row['card_num'].strip(),
                #     False,
                #     None,
                # )
                # cur.execute(insert_query, record_to_insert)
        try:
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cur.close()
        cur.close()
    return