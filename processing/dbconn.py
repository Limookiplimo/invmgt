import psycopg2

def connect_db():
    try:
        with psycopg2.connect(host='localhost',port=5432, database='database', user='username', password='password') as conn:
            print(" Connceted successfuly")
            return conn
    except psycopg2.Error as e:
        print(" Connection error: ", e)
        return None