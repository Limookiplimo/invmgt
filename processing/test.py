from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Kafka properties
consumer_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink_consumer_group",
    "auto.offset.reset": "latest" 
}
producer_props = {
    "bootstrap.servers": "localhost:9092"
}
input_topic = "orders"
output_topic = "sales"
schema = SimpleStringSchema()

kafka_source = KafkaSource \
        .builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_group_id("flink_consumer_group") \
        .set_topics('input_topic') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


# Data source
stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# Relevant sales data
parsed_stream = stream \
    .map(lambda msg: eval(msg)) \
    .map(lambda msg: (msg["id"], msg["amount"],msg["event_time"]))

# Stateful processing
sales = parsed_stream \
    .key_by(lambda x: x[0]) \
    .reduce(lambda x, y: (x[0], x[1] + y[1], max(x[2], y[2])))

# Print results
sales.print()

# # Data sink
# sales.add_sink(kafka_producer)

# Execute the job
env.execute("Kafka Order Processing")
