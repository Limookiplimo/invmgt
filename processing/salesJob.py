from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowSerializationSchema
import json


def process_orders():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Kafka Properties
    bootstrap_servers = 'localhost:9092'
    flink_group = "flink_consumer_group"
    input_topic = "orders"
    output_topic = "sales"

    schema = SimpleStringSchema()
    kafka_source = KafkaSource \
            .builder() \
            .set_bootstrap_servers(bootstrap_servers) \
            .set_group_id(flink_group) \
            .set_topics(input_topic) \
            .set_value_only_deserializer(schema) \
            .build()

    # Data source
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), source_name=input_topic)

    sales_json = stream \
            .map(lambda msg: json.loads(msg)) \
            .key_by(lambda x: x["id"]) \
            .reduce(lambda x, y: {"id": x["id"], "amount": x["amount"] + y["amount"], "event_time": max(x["event_time"], y["event_time"])})

    # JSON serialization schema
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.INT(), Types.INT(), Types.STRING()])).build()

    # Sink to Kafka
    kafka_producer = FlinkKafkaProducer(
        topic=output_topic,
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "localhost:9092", "group.id": "flink_kafka_producer"})

    sales_json.add_sink(kafka_producer)

    # Execute the job
    env.execute("Kafka Order Processing")

if __name__ == "__main__":
    process_orders()