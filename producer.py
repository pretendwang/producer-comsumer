from kafka import KafkaProducer,TopicPartition
import time
import uuid
producer = KafkaProducer(bootstrap_servers='localhost:9092')
display_interval = 5

print('正在给assign_topic发送消息')
display_iteration = 0
message_count = 0
start_time = time.time()
while True:
    identifier = str(uuid.uuid4())
    producer.send('assign_topic', identifier.encode('utf-8'))
    message_count += 1
    now = time.time()
    if now - start_time > display_interval:
        print('第%i个:%i个消息以每秒%.0f个消息生产' % (
            display_iteration,
            message_count,
            message_count / (now - start_time)))
        display_iteration += 1
        message_count = 0
        start_time = time.time()