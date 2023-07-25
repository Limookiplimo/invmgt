"""
- create database : DONE
- load data to dabase: DONE
- extract data from database and publish to kafka topic: DONE
- perform stateful processing with pyflink and write to these topics: ON PROGRESS
        - invoices - [inv_id, csm_code, prd_id, amount]
        - sales - [inv_count, amount]
        - logistics - [total_weight, vehicles]
- vizualize and monitor
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import ScalarFunction
from pyflink.table.window import Tumble
from confluent_kafka import Producer
import psycopg2
import json
import datetime


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


# Producer
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
            'amount': row[7]
        }
        serialize = json.dumps(message)
        kafka_producer.produce(topic, value=serialize)

    kafka_producer.flush()
try:   
    sales_data = get_data()
    order_producer(sales_data)
    print("Data published to Kafka successfully. \n")
    
except Exception as e:
    print(f"Producer error: ", e)
    

def stream_sales():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)

    # Kafka source table
    table_env.execute_sql("""
        CREATE TABLE orders_table (
            id BIGINT,
            customer_code STRING,
            orderdate DATE,
            ordertime TIME,
            product_code STRING,
            quantity INT,
            weight FLOAT,
            amount FLOAT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_consumer_group',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
    """)
  
    # create and initiate loading of source Table
    orders_tbl = table_env.from_path('orders_table')

    print('\n Source Schema')
    orders_tbl.print_schema()

# Register a table function to extract the timestamp from the 'orderdate' and 'ordertime' fields
    class ExtractTimestamp(ScalarFunction):
        def eval(self, orderdate, ordertime):
            date_time_str = f"{orderdate} {ordertime}"
            return datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    table_env.create_temporary_system_function("ExtractTimestamp", ExtractTimestamp())

    # Use the registered function to extract the timestamp and assign it as the event time
    orders_tbl = orders_tbl.select("id, customer_code, product_code, quantity, weight, amount, ExtractTimestamp(orderdate, ordertime) as event_time")

    # Define a tumbling window with a size of 10 minutes and an offset of 0
    tumbling_window = Tumble.over("10.minutes").on("event_time").alias("w")

    # Perform the aggregations over the tumbling window
    agg_query = """
        SELECT 
            TUMBLE_START(event_time, INTERVAL '10' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '10' MINUTE) as window_end,
            SUM(amount) as total_amount
        FROM orders_table
        GROUP BY TUMBLE(event_time, INTERVAL '10' MINUTE)
"""
    table_env.execute_sql(agg_query).print()

if __name__ == "__main__":
    try:   
        sales_data = get_data()
        order_producer(sales_data)
        print("Data published to Kafka successfully.\n")
        print(sales_data)
    except Exception as e:
        print(f"Producer error: ", e)

    stream_sales()