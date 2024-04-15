from confluent_kafka.admin import AdminClient, NewTopic

class MyTopics():
    
    def __init__(self, server):
        self.servers = server
        self.admin = AdminClient({ 'bootstrap.servers': server })

    def create_topic(self, topic_name):
        cluster_metadata = self.list_topics()
        topics = cluster_metadata.topics
        if topic_name not in topics:
            self.admin.create_topics([NewTopic(topic=topic_name, num_partitions=1)])
        return True
    
    def list_topics(self):
        return self.admin.list_topics(timeout=10)