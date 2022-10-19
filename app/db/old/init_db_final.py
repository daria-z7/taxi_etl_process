import psycopg2


conn = psycopg2.connect(host='de-edu-db.chronosavant.ru',
                        port=5432,
                        database='dwh',
                        user='dwh_moscow',
                        password='dwh_moscow_password')

cur = conn.cursor()

cur.execute('CREATE TABLE dim_drivers (personnel_num CHAR(9),'
            'start_dt TIMESTAMP(0),'
            'last_name CHAR(20),'
            'first_name CHAR(20),'
            'middle_name CHAR(20),'
            'birth_dt DATE,'
            'card_num CHAR(19),'
            'driver_license_num CHAR(12),'
            'driver_license_dt DATE,'
            'deleted_flag CHAR(1),'
            'end_dt TIMESTAMP(0),'
            'PRIMARY KEY (personnel_num,start_dt);)'
            )

cur.execute('CREATE TABLE dim_cars (plate_num CHAR(9),'
            'start_dt TIMESTAMP(0),'
            'model_name CHAR(20),'
            'revision_dt DATE,'
            'deleted_flag CHAR(1),'
            'end_dt TIMESTAMP(0),'
            'PRIMARY KEY (plate_num,start_dt);)'
            )

cur.execute('CREATE TABLE dim_clients (phone_num serial PRIMARY KEY (phone_num, start_dt),'
            'card_num CHAR(19),'
            'deleted_flag CHAR(1),'
            'end_dt TIMESTAMP(0);)'
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
            'FOREIGN KEY (car_plate_num) REFERENCES dim_cars (plate_num);)'

            )

cur.execute('CREATE TABLE  fact_waybills (waybill_num CHAR(12),'
            'driver_pers_num CHAR(9),'
            'car_plate_num CHAR(9),'
            'work_start_dt TIMESTAMP(0),'
            'work_end_dt TIMESTAMP(0),'
            'issue_dt DATE,'
            'FOREIGN KEY (driver_pers_num) REFERENCES dim_drivers(personnel_num),'
            'FOREIGN KEY (car_plate_num) REFERENCES dim_cars(plate_num),'
            'PRIMARY KEY (waybill_num);)'
            )

cur.execute('CREATE TABLE fact_payments (transaction_id serial PRIMARY KEY,'
            'card_num CHAR(19),'
            'card_num CHAR(19),'
            'transaction_amt NUMERIC(7,2),'
            'transaction_dt TIMESTAMP(0);)'
            )

conn.commit()
cur.close()
conn.close()
