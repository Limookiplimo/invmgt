from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.window import Tumble
import psycopg2
from kafka import KafkaProducer, KafkaConsumer


# Stream processing
def process_orders(value, ctx, runtime_context):
    # State descriptors
    total_amount_descriptor = ValueStateDescriptor("total_amount", DataTypes.FLOAT())
    total_weight_descriptor = ValueStateDescriptor("total_weight", DataTypes.FLOAT())
    transaction_count_descriptor = ValueStateDescriptor("transaction_count", DataTypes.BIGINT())

    # State handles
    total_amount_state = runtime_context.get_state(total_amount_descriptor)
    total_weight_state = runtime_context.get_state(total_weight_descriptor)
    transaction_count_state = runtime_context.get_state(transaction_count_descriptor)

    # Current state
    current_total_amount = total_amount_state.value() or 0.0
    current_total_weight = total_weight_state.value() or 0.0
    current_transaction_count = transaction_count_state.value() or 0

    # Add incoming values
    current_total_amount += float(value.split(',')[1])
    current_total_weight += float(value.split(',')[2])
    current_transaction_count += int(value.split(',')[3])

    # Update the state values
    total_amount_state.update(current_total_amount)
    total_weight_state.update(current_total_weight)
    transaction_count_state.update(current_transaction_count)

    # Emit updated values
    return f"{value.split(',')[0]},{current_total_amount},{current_total_weight},{current_transaction_count}"

  
# PyFlink Job
def publish_to_kafka():
    # Flink kafka consumer
    kafka_consumer = KafkaConsumer(
            topics='processed_orders',
            deserialization_schema=SimpleStringSchema(),
            properties={'bootstrap_servers': 'localhost:9092', 'group.id':'sales-orders'}
    )

    # Flink kafka producer
    kafka_producer = KafkaProducer(
        topic='unprocessed_orders',
        serialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092'}
    )

    # Path to the Flink Kafka connector JAR file
    kafka_connector_jar = '/env/lib/python3.10/site-packages/pyflink/lib/flink-connector-files-1.17.1.jar'


    # Execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Database connection
    try:
        with psycopg2.connect(host='localhost', port=5432, database='database', user='username', password='password') as conn:
            print("Successfully connected")
            cursor = conn.cursor()

            # Fetch data from the "orders" table
            query = "SELECT * FROM orders;"
            cursor.execute(query)
            orders = cursor.fetchall()

            # Send each order to the Kafka topic
            for order in orders:
                kafka_producer.send(value=','.join(str(item) for item in order))

            # Flush and close the producer
            kafka_producer.flush()
            kafka_producer.close()

    except psycopg2.Error as e:
        print("Connection error:", e)

    # Processing pipeline
    env.add_source(kafka_consumer)\
        .map(process_orders)\
        .add_sink(kafka_producer)

    # Execute job
    env.execute("Stateful Sales Order Processing")

publish_to_kafka()