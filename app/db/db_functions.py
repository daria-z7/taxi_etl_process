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
        print(trans_id[0])
    cur.close()
    conn.close()
    return 0 if trans_id is None else trans_id[0]


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
