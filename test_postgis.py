import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="env_curated",
        user="postgis",
        password="postgis"
    )
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {str(e)}")