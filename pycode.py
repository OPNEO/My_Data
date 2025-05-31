from kafka import KafkaConsumer
from json import loads
consumer=KafkaConsumer('customer_data_topic_new',
                       bootstrap_servers=['localhost:9092'],
                       value_deserializer=lambda x:loads(x.decode('utf-8')),
                       enable_auto_commit=True,
                       auto_offset_reset='earliest',
                       group_id='group_12')
for x in consumer:
    print(x.value)
# from kafka.admin import KafkaAdminClient
# admin=KafkaAdminClient(bootstrap_servers=['localhost:9092'])
# admin.delete_topics(['customer_data_topic_new'])