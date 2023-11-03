import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                         value_serializer = lambda x: json.dumps(x).encode('utf-8'))
with open(r"data.json", "r")as a:
    data = json.load(a)
    for datas in data:
        producer.send('data',value=datas)
        print('data added')

producer.close()





