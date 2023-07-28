from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource,KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
import psycopg2
import json
from datetime import date, time as datetime_time

# Custom JSON Encoder to handle date and time objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (date, datetime_time)):
            return o.isoformat()
        return super().default(o)

# Extract data
def get_data():
    try:
        with psycopg2.connect(
                host='localhost',
                port=5432,
                database='database',
                user='username',
                password='password') as conn:
            print("\n Successfully connected \n ")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM orders;")
            orders = cursor.fetchall()
            cursor.close()
        return orders
    except psycopg2.Error as e:
        print(f"Connection error: ", e)
        return None

# Kafka producer
def order_producer(data):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_sink = KafkaSink \
        .builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(JsonRowSerializationSchema.builder().build()) \
        .delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # Data Transformation and Sink
    env.from_collection(data) \
        .map(lambda x: json.dumps(x, default=str)) \
        .sink_to(kafka_sink)

    # Execute the Flink job
    env.execute("Write orders to Kafka")

try:
    sales_data = get_data()
    if sales_data:
        order_producer(sales_data)
        print("Data published to Kafka successfully. \n")
    else:
        print("No data retrieved from the database. \n")
    
except Exception as e:
    print(f"Producer error: ", e)
