from kafka import KafkaConsumer,TopicPartition
import time
import uuid

display_interval = 5

consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer2.assign([TopicPartition('assign_topic', 1)])
print('正在从assign_topic接收消息')
display_iteration = 0
message_count = 0
partitions = set()
start_time = time.time()
while True:
    message = next(consumer2)
    identifier = str(message.value,encoding="utf-8")
    message_count += 1
    partitions.add(message.partition)
    now = time.time()
    if now - start_time > display_interval:
        print('第%i:%i个消息以每秒%.0f个消息消费-来自分区:%r'% (
            display_iteration,
            message_count,
            message_count / (now - start_time),
            sorted(partitions)))
        display_iteration += 1
        message_count = 0
        partitions = set()
        start_time = time.time()