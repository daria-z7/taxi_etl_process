import psycopg2


conn = psycopg2.connect(host='de-edu-db.chronosavant.ru',
                        port=5432,
                        database='dwh',
                        user='dwh_moscow',
                        password='dwh_moscow_password')

cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS fact_payments;')
cur.execute('CREATE TABLE fact_payments (transaction_id serial PRIMARY KEY,'
                                 'card_num integer NOT NULL,'
                                 'transaction_amount decimal NOT NULL,'
                                 'transaction__dt date NOT NULL;'
                                 )

conn.commit()
cur.close()
conn.close()