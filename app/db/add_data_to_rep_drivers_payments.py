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

def load_fio_drivers_card_amt():
    conn_2  = create_conn_2()
    cur = conn_2.cursor()
    with cur as cur:
        cur.execute(""" SELECT 	fr.driver_pers_num, max(dd.last_name) last_name, 
                            		max(dd.first_name) first_name, 
                            		max(dd.middle_name) middle_name,
                            		max(dd.card_num) card_num, 
                            		round(sum(fr.price_amt - (fr.price_amt*.2 + 47.26*7*fr.distance_val/100 + 5*fr.distance_val)), 2) as amount
                        FROM fact_rides fr left join dim_drivers dd on fr.driver_pers_num = dd.personnel_num
                        WHERE DATE(ride_end_dt) = CURRENT_DATE - 4
                        group by fr.driver_pers_num
                        having fr.driver_pers_num != '2147483647'
                    """)
        query = cur.fetchall()
    df = pd.DataFrame(query, columns= ['personnel_num', 'last_name', 'first_name',  'middle_name', 'card_num', 'amount'])
    #cur.close()
    conn_2.close()
    #print(type(df))
    return df


def add_data_to_rep_drivers_payments(df: list):
    if not df.empty:
        conn_2 = create_conn_2()
        cur = conn_2.cursor()
        report_dt = date.fromordinal(date.today().toordinal() - 3)
        for index, row in df.iterrows():
            # arrival_dt = get_arrival_dt(row['ride_id'], 'READY')
            # start_dt = get_arrival_dt(row['ride_id'], 'BEGIN')
            # personnel_num = get_personnel_num(row['car_plate_num'])
            # print('good')
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


if __name__ == "__main__":
    df = load_fio_drivers_card_amt()
    print(df)
    add_data_to_rep_drivers_payments(df)
    print('Successuful')