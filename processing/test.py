from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def stream_sales():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)

    table_env.execute_sql("""
            CREATE TABLE orders_table (
                id BIGINT,
                customer_code STRING,
                amount FLOAT,
                event_time TIMESTAMP
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'orders',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'flink_consumer_group',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false'
                )
            """)
    
    

    