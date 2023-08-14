curl -X POST https://postgres-check-service.sprint9.tgcloudenv.ru/init_schemas \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
  "student": "leonidgrishenkov",
  "pg_settings": {
    "host": "rc1b-ma4myydk2njfnxfy.mdb.yandexcloud.net",
    "port": 6432,
    "dbname": "sprint9dwh",
    "username": "dwh_admin",
    "password": "QLGa7RzpKdeQK6y9XGmi",
    "sslrootcert": "/Users/leonidgrisenkov/Code/de-sprint-9/CA.pem"
  }
}
EOF