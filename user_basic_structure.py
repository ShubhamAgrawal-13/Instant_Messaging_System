from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from colorama import Fore

import threading

def handle_consume(topic):
	consumer = KafkaConsumer(topic,
							bootstrap_servers=['localhost:9092'],
							auto_offset_reset='earliest',
							value_deserializer=lambda x: loads(x.decode('utf-8')))


	for message in consumer:
		value=message.value
		print(Fore.GREEN+"[New Message] : ",value)
		print(Fore.WHITE)



def handle_produce(topic):
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
	while(1):
		msg=input("Enter something : ")
		producer.send(topic, key=topic, value=msg)

t1=threading.Thread(target=handle_produce,args=("user2_to_user1",))
t1.start()
t2=threading.Thread(target=handle_consume,args=("user1_to_user2",))
t2.start()