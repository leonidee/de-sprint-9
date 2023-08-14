import psycopg2

def main():

    conn = psycopg2.connect("""
        host=rc1b-ma4myydk2njfnxfy.mdb.yandexcloud.net
        port=6432
        sslmode=verify-full
        dbname=sprint9dwh
        user=dwh_admin
        password=QLGa7RzpKdeQK6y9XGmi
        target_session_attrs=read-write
        sslrootcert='/Users/leonidgrisenkov/Code/de-sprint-9/CA.pem'
    """)

    q = conn.cursor()
    q.execute('SELECT schema_name  FROM information_schema.schemata;')

    print(q.fetchall())

    conn.close()



if __name__ == "__main__":
    main()