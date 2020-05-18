from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from colorama import Fore
import getpass
import socket

from sys import argv
import threading
import os

msg_list={}
#msg_latest={}

mutex=1


from pymongo import MongoClient 
import json
  
try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
except:   
    print("Could not connect to MongoDB") 

db = conn['Msg_db']
msgs=db['msgs']

logged_in=0


def initialize(user_id):
	global msg_list
	for record in msgs.find({"user":user_id}):
		msg_list=record["msg"]

def update_db_msg(user_id):
	global msg_list
	myquery = { "user": user_id }
	newvalues = { "$set": { "msg": msg_list }  }
	msgs.update_one(myquery, newvalues)

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
		if(key=="bye"):
			break
		lock()
		#print(key,value)
		#beep()
		if(key not in msg_list):
			msg_list[key]=[]
			msg_list[key].append(value)
		else:
			msg_list[key].append(value)
		update_db_msg(user_id)
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

		if(cmds[0]=="logout"):
			producer.send(user_id, key=b'bye', value='bye')
			#s.send(b"logout")
			break


def main():
	#topic=str(argv[1])
	while(1):
		print("----- Instant Messaging System : -------")
		print("Commnds Lists : \n login \n logout \n sigin \n exit \n send \n read \n")
		cmd=input("Enter Command")

		if(cmd=="login"):
			user=input("Enter username : ")
			pwd=getpass.getpass(prompt='Enter password : ')
			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server = "192.168.1.3"
			port = 5544
			addr = (server, port) 
			client.connect(addr)
			# 	print("Connected to server")
			# else:
			# 	print("No server first start server")
			# 	break
			msg=client.recv(1024).decode()
			print(msg)
			msg=str(user)+":"+str(pwd)
			client.send(str.encode(msg))
			f=client.recv(1024).decode()
			if(f=="ok"):
				print("Successfully logged in")
				initialize(user)
				print("[Instant Messaging System] : USER_ID : ",user)
				t1=threading.Thread(target=handle_produce,args=(user,))
				t1.start()
				t2=threading.Thread(target=handle_consume,args=(user,))
				t2.start()
				t1.join()
				#t2.join()
			else:
				print("Unsuccessful")

		elif(cmd=="sign_in"):
			user=input("Enter username : ")
			pwd=getpass.getpass(prompt='Enter password : ') 
			#send(user,pwd)
			print("Successfully Registered")

		elif(cmd=="exit"):
			print("Bye")
			break

		else:
			print("---------- First log in ----------")
#Instant_Messaging_System
if __name__ == '__main__':
	main()