import msgpack
from kafka import KafkaConsumer
consumer = KafkaConsumer('my_favorite_topic')
for msg in consumer:
    print (msg)

# join a consumer group for dynamic partition assignment and offset commits
from kafka import KafkaConsumer
consumer = KafkaConsumer('my_favorite_topic', group_id='my_favorite_group')
for msg in consumer:
    print (msg)

# manually assign the partition list for the consumer
from kafka import TopicPartition
consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
consumer.assign([TopicPartition('foobar', 2)])
msg = next(consumer)

# Deserialize msgpack-encoded values
consumer = KafkaConsumer(value_deserializer=msgpack.loads)
consumer.subscribe(['msgpackfoo'])
for msg in consumer:
    assert isinstance(msg.value, dict)

# Access record headers. The returned value is a list of tuples
# with str, bytes for key and value
for msg in consumer:
    print (msg.headers)

# Read only committed messages from transactional topic
consumer = KafkaConsumer(isolation_level='read_committed')
consumer.subscribe(['txn_topic'])
for msg in consumer:
    print(msg)

# Get consumer metrics
metrics = consumer.metrics()
for metric in metrics:
    print(metric)