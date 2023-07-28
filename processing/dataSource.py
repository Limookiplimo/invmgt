from confluent_kafka import Producer
import psycopg2
import json
import time


# Extract data
def get_data():
    try:
        with psycopg2.connect(host='localhost', 
                            port=5432, 
                            database='database', 
                            user='username', 
                            password='password') as conn:
            print("\n Successfully connected \n ")
    except psycopg2.Error as e:
        print(f"Connection error: ", e)

    cursor = conn.cursor()
    cursor.execute("Select * from orders;")
    orders = cursor.fetchall()
    cursor.close()
    conn.close()
    return orders

# Kafka roducer
def order_producer(data):
    bootstrap_servers = 'localhost:9092'
    topic = 'orders'
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'producer_flink'
    }
    kafka_producer = Producer(producer_conf)
    for row in data:
        message = {
            'id': row[0],
            'customer_code': row[1],
            'orderdate': row[2].strftime('%Y-%m-%d'),
            'ordertime': row[3].strftime('%H:%M:%S'),
            'product_code': row[4],
            'quantity': row[5],
            'weight': row[6],
            'amount': row[7],
            'event_time': row[8].strftime('%Y-%m-%d %H:%M:%S')
        }
        serialize = json.dumps(message)
        kafka_producer.produce(topic, value=serialize)
        print(serialize)
        time.sleep(1)

    kafka_producer.flush()
    

try:
    sales_data = get_data()
    order_producer(sales_data)
    print("Data published to Kafka successfully. \n")
    
except Exception as e:
    print(f"Producer error: ", e)