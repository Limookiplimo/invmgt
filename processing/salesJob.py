from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee

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

    kafka_sink = KafkaSink\
            .builder()\
            .set_bootstrap_servers("localhost:9092") \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(output_topic)
                .set_value_serialization_schema(schema)
                .build()) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    # Data source
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), input_topic)

    # Data processing
    parsed_stream = stream \
        .map(lambda msg: eval(msg)) \
        .map(lambda msg: (msg["id"], msg["amount"], msg["event_time"]))

    sales = parsed_stream \
        .key_by(lambda x: x[0]) \
        .reduce(lambda x, y: (x[0], x[1] + y[1], max(x[2], y[2])))

    # Sink topic
    sales.sink_to(kafka_sink)

    # Print results
    sales.print()

    # Execute the job
    env.execute("Kafka Order Processing")

if __name__ == "__main__":
    process_orders()