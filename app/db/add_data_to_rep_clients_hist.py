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

def load_clients_hist():
    conn_2  = create_conn_2()
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
    #cur.close()
    conn_2.close()
    #print(type(df))
    return df


def add_data_to_rep_clients_hist(df: list):
    if not df.empty:
        conn_2 = create_conn_2()
        cur = conn_2.cursor()
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


if __name__ == "__main__":
    df = load_clients_hist()
    print(df)
    add_data_to_rep_clients_hist(df)
    print('Successuful')


