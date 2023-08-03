from confluent_kafka import Consumer, KafkaError
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import time

def consume_topic():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        bootstrap_servers = 'localhost:9092'
        input_topic = 'sales'
        group_id = 'flink_consumer_group'

        kafka_source = KafkaSource\
                .builder()\
                .set_bootstrap_servers(bootstrap_servers)\
                .set_topics(input_topic)\
                .set_group_id(group_id)\
                .set_starting_offsets(KafkaOffsetsInitializer.earliest())\
                .set_value_only_deserializer(SimpleStringSchema())\
                .build()
        
        msg = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), input_topic)

        msg.print()
        time.sleep(1)
        env.execute("Read Kafka Topic")
    except Exception as e:
        print(f"Topic Error: {e}")

if __name__ == '__main__':
    consume_topic()
