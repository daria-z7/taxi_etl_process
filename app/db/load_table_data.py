import psycopg2


shema = 'main'

conn = psycopg2.connect(
    host='de-edu-db.chronosavant.ru',
    port=5432,
    database='taxi',
    user='etl_tech_user',
    password='etl_tech_user_password',
    options=f'-c search_path={shema}',
)
cur = conn.cursor()
with cur as cur:
    cur.execute('SELECT * FROM drivers')
    print(cur.fetchall())
cur.close()
conn.close()