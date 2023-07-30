from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
import json

def parse_json(msg):
    try:
        data = json.loads(msg)
        return data["id"], data["amount"], data["event_time"]
    except json.JSONDecodeError as e:
        print(f" Json decode error: ", e)
        return None

def process_orders():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Properties
    input_topic = "orders"
    output_topic = "sales"
    schema = SimpleStringSchema()

    kafka_source = KafkaSource \
            .builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_group_id("flink_consumer_group") \
            .set_topics(input_topic) \
            .set_value_only_deserializer(schema) \
            .build()

    # Data source
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), input_topic)

    # Data processing
    parsed_stream = stream.map(parse_json).filter(lambda x: x is not None)

    sales = parsed_stream \
        .key_by(lambda x: x[0]) \
        .reduce(lambda x, y: (x[0], x[1] + y[1], max(x[2], y[2])))

    # Convert to JSON
    sales_json = sales.map(lambda x: json.dumps({"id":x[0],"total_amount":x[1],"max_event_time":x[2]}))

    record_serializer = KafkaRecordSerializationSchema.builder() \
            .set_topic(output_topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .set_key_serialization_schema(SimpleStringSchema()) \
            .build()
    
    kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_record_serializer(record_serializer) \
            .build()
    
    sales_json.add_sink(kafka_sink)

    # Execute the job
    env.execute("Kafka Order Processing")

if __name__ == "__main__":
    process_orders()