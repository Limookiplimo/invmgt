import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json
from dbconn import connect_db

# Flink parameters
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'sales_topic'

def process_orders():
    # Create a Flink execution environment and table environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Define the PostgreSQL query to stream sales data
    query = "SELECT * FROM orders"

    # Configure Flink to use Kafka as the data source
    t_env \
        .connect(  # Connect to the data source
            Kafka()
            .version("universal")
            .topic(KAFKA_TOPIC)
            .property("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
            .property("zookeeper.connect", "localhost:2181")
        ) \
        .with_format(  # Specify the format of the data
            Json()
            .json_schema(
                "{"
                "  'type': 'object',"
                "  'properties': {"
                "    'sale_id': {'type': 'integer'},"
                "    'product': {'type': 'string'},"
                "    'amount': {'type': 'double'},"
                "    'timestamp': {'type': 'string', 'format': 'date-time'}"
                "  }"
                "}"
            )
            .fail_on_missing_field(True)
        ) \
        .with_schema(  # Specify the schema of the data
            Schema()
            .field("sale_id", DataTypes.INT())
            .field("product", DataTypes.STRING())
            .field("amount", DataTypes.DOUBLE())
            .field("timestamp", DataTypes.TIMESTAMP())
        ) \
        .in_append_mode() \
        .register_table_source("sales_source")

    # Define a table from the registered source
    sales_table = t_env.from_path("sales_source")

    # Perform computations on the sales table
    result_table = sales_table.group_by("product") \
        .select("product, amount.sum as total_amount")

    # Define a sink to print the result to the console
    result_table \
        .to_retract_stream() \
        .print()

    # Execute the Flink job
    env.execute("Sales Data Streaming")

if __name__ == '__main__':
    process_orders()
