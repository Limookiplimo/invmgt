from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.types import Row
import json


def process_orders():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    schema = SimpleStringSchema()
    bootstrap_servers = 'localhost:9092'
    flink_group = "flink_consumer_group"
    input_topic = "orders"
    output_topic = "sales"


    kafka_source = KafkaSource \
        .builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_group_id(flink_group) \
        .set_topics(input_topic) \
        .set_value_only_deserializer(schema) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), source_name=input_topic)

    parsed_stream = stream \
        .map(lambda msg: json.loads(msg)) \
        .map(lambda msg: (msg["id"], msg["amount"], msg["event_time"]))

    sales_json = parsed_stream \
        .key_by(lambda x: x[0]) \
        .reduce(lambda x, y: (x[0], x[1] + y[1], max(x[2], y[2])))

    # # Convert Python tuple to Flink Row
    # sales_data = sales_json.map(lambda x: Row(x[0], x[1], x[2]))

    sales = sales_json \
        .map(lambda x: json.dumps({"id": x[0], "amount": x[1], "event_time": x[2]}))

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.INT(), Types.INT(), Types.STRING()])).build()

    kafka_producer = FlinkKafkaProducer(
        topic=output_topic,
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "localhost:9092", "group.id": "flink_kafka_producer"})

    sales.add_sink(kafka_producer)

    env.execute("Kafka Order Processing")

if __name__ == "__main__":
    process_orders()

 
