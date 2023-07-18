from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.window import Tumble
import psycopg2

# Execution and Table environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Create orders table
t_env.execute_sql("""
    CREATE TABLE orders (
        ID INT,
        CSMCODE STRING,
        ORDDATE DATE,
        ORDTIME TIMESTAMP(3),
        PRDTID INT,
        QUANTITY INT,
        WEIGHT DOUBLE,
        AMOUNT DOUBLE,
        WATERMARK FOR ORDTIME AS ORDTIME - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'psycopg2',
        'host' = 'localhost',
        'port' = '5432',
        'database' = 'database',
        'table-name' = 'orders',
        'username' = 'username',
        'password' = 'password'
    )
""")
# Aggregation statement
aggregation_query = """
    SELECT
        TUMBLE_START(ORDTIME, INTERVAL '30' MINUTE) as window_start,
        SUM(AMOUNT) as total_amount,
        SUM(WEIGHT) as total_weight,
        COUNT(*) as transaction_count
    FROM orders
    GROUP BY TUMBLE(ORDTIME, INTERVAL '30' MINUTE)
"""

# Register aggregation statement as view
t_env.execute_sql("CREATE VIEW aggregation_view AS " + aggregation_query)

# Stream processing
class OrderProcessing(KeyedProcessFunction):
    def open(self, runtime_context):
        # State descriptors
        total_amount_descriptor = ValueStateDescriptor("total_amount", Types.DOUBLE())
        total_weight_descriptor = ValueStateDescriptor("total_weight", Types.DOUBLE())
        transaction_count_descriptor = ValueStateDescriptor("transaction_count", Types.LONG())

        # State handles
        self.total_amount_state = runtime_context.get_state(total_amount_descriptor)
        self.total_weight_state = runtime_context.get_state(total_weight_descriptor)
        self.transaction_count_state = runtime_context.get_state(transaction_count_descriptor)
    
    def process_orders(self, value, ctx):
        # Current state
        current_total_amount = self.total_amount_state.value() or 0.0
        current_total_weight = self.total_weight_state.value() or 0.0
        current_transaction_count = self.transaction_count_state.value() or 0

        # Add incoming values
        current_total_amount += value["total_amount"]
        current_total_weight += value["total_weight"]
        current_transaction_count += value["transaction_count"]

        # Update the state values
        self.total_amount_state.update(current_total_amount)
        self.total_weight_state.update(current_total_weight)
        self.transaction_count_state.update(current_transaction_count)

        # Emit updated values
        ctx.output((value["window_start"], current_total_amount, current_total_weight, current_transaction_count))

# Datastream type information
type_info = Types.ROW([Types.SQL_TIMESTAMP(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()])
t_env.get_config().get_configuration().set_string("python.fn-execution.results-mode", "changelog")
t_env.get_config().get_configuration().set_string("python.fn-execution.max-buffered-size", "1")

# Aggregation view to Datastream
data_stream = t_env.to_data_stream(t_env.sql_query("SELECT * FROM aggregation_view"), type_info)

# Key the DataStream by the window start time
data_stream = data_stream.key_by(lambda x: x[0])

# Process the DataStream
data_stream.process(OrderProcessing()).print()

# Execute the Flink job
env.execute()
