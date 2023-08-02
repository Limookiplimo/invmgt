from confluent_kafka.admin import AdminClient, NewTopic


bootstrap_servers = "localhost:9092"
topic_name = "sales"
num_partitions = 3

replication_factor = 1

def create_topic():
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    topics = [new_topic]
    fs = admin_client.create_topics(topics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_topic()
