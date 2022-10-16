import psycopg2


conn = psycopg2.connect(host='de-edu-db.chronosavant.ru',
                        port=5432,
                        database='dwh',
                        user='dwh_moscow',
                        password='dwh_moscow_password')

cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS fact_rides;')
cur.execute('CREATE TABLE dim_clients (phone_num serial PRIMARY KEY,'
            'start_dt serial PRIMARY KEY,'
            'card_num CHAR(19),'
            'deleted_flag CHAR(1),'
            'end_dt TIMESTAMP(0);'
            )

cur.execute('CREATE TABLE fact_rides (ride_id serial PRIMARY KEY,'
            'point_from_txt VARCHAR(200),'
            'point_to_txt VARCHAR(200),'
            'distance_val NUMERIC(5,2),'
            'price_amt NUMERIC(7,2),'
            'client_phone_num CHAR(18),'
            'driver_pers_num CHAR(12),'
            'car_plate_num CHAR(9),'
            'ride_arrival_dt TIMESTAMP(0),'
            'ride_start_dt TIMESTAMP(0),'
            'ride_end_dt TIMESTAMP(0),'
            'FOREIGN KEY (client_phone_num) REFERENCES dim_clients (phone_num),'
            'FOREIGN KEY (drivers_pers_num) REFERENCES dim_drivers (personnel_num),'
            'FOREIGN KEY (car_plate_num) REFERENCES dim_cars (plate_num);'

            )

cur.execute('CREATE TABLE fact_payments (transaction_id serial PRIMARY KEY,'
            'card_num CHAR(19),'
            'card_num CHAR(19),'
            'transaction_amt NUMERIC(7,2),'
            'transaction_dt TIMESTAMP(0);'
            )

conn.commit()
cur.close()
conn.close()
