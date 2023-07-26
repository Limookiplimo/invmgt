from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Kafka consumer properties
kafka_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink_consumer_group",
    "auto.offset.reset": "latest" 
}

# Kafka topic and schema
topic = "orders"
schema = SimpleStringSchema()

# FlinkKafkaConsumer to read from the Kafka topic
kafka_consumer = FlinkKafkaConsumer(topic, schema, properties=kafka_props)

# Kafka consumer as the data source
stream = env.add_source(kafka_consumer)

# Parse the JSON messages and filter only the relevant fields (id and amount)
parsed_stream = stream \
    .map(lambda msg: eval(msg)) \
    .map(lambda msg: (msg["id"], msg["amount"]))

# Key-by operation to group by the 'id' field, and calculate the sum of the 'amount' field
sum_amounts = parsed_stream \
    .key_by(lambda x: x[0]) \
    .reduce(lambda x, y: (x[0], x[1] + y[1]))

# Print the result on the screen
sum_amounts.print()

# Execute the Flink job
env.execute("Kafka Order Processing")
