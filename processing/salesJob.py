from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


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

kafka_producer = FlinkKafkaProducer(output_topic, schema, properties=producer_props)
kafka_consumer = FlinkKafkaConsumer(input_topic, schema, properties=consumer_props)

# Data source
stream = env.add_source(kafka_consumer)

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

# Data sink
sales.add_sink(kafka_producer)

# Execute the job
env.execute("Kafka Order Processing")
