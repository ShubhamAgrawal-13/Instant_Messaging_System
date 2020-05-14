from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from colorama import Fore

from sys import argv
import threading
import os

msg_list={}
#msg_latest={}

mutex=1

def lock():
	global mutex
	while(mutex<=0):
		pass
	mutex=0

def unlock():
	global mutex
	mutex=1

def beep():
	os.system("play -nq -t alsa synth 1 sine 600")


def read_msg(_user):
	global msg_list
	lock()
	if(_user=="all"):
		for k,v in msg_list.items():
			print(Fore.GREEN+"Messages from user : ", k)
			print(Fore.WHITE)
			for msg in v:
				print(msg)
			print()
	else:
		print(Fore.GREEN+"Messages from user : ", _user)
		print(Fore.WHITE)
		for msg in msg_list[_user]:
			print(msg)
		print()
	unlock()


def handle_consume(user_id):
	global msg_list
	consumer = KafkaConsumer(user_id,
							bootstrap_servers=['localhost:9092'],
							auto_offset_reset='earliest',
							value_deserializer=lambda x: loads(x.decode('utf-8')))


	for message in consumer:
		key=message.key.decode('utf-8')
		value=message.value
		lock()
		#print(key,value)
		beep()
		if(key not in msg_list):
			msg_list[key]=[]
			msg_list[key].append(value)
		else:
			msg_list[key].append(value)
		unlock()

def handle_produce(user_id):
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
	while(1):
		inp=input("---> ")
		cmds=inp.split(" ")

		if(cmds[0]=="send"):
			msg=input("Enter the message : ")
			producer.send(cmds[1], key=user_id.encode("utf-8"), value=msg)

		if(cmds[0]=="read"):
			read_msg(cmds[1])





def main():
	#topic=str(argv[1])
	print("[Instant Messaging System] : USER_ID : ",argv[1])
	t1=threading.Thread(target=handle_produce,args=(argv[1],))
	t1.start()
	t2=threading.Thread(target=handle_consume,args=(argv[1],))
	t2.start()

#Instant_Messaging_System
if __name__ == '__main__':
	main()