#!/usr/bin/python3

from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from colorama import Fore

import threading

users={}
login=0
contacts={}

current_user=""
producer=""

def add_contact(topic):
	global users
	global login
	global current_user
	global contacts
	contacts[current_user].append(topic)
	f = open("users_contact_list.txt", "w")
	for k,v in contacts.items():
		f.write(str(k)+" ")
		for vv in v.items():
			f.write(str(vv)+" ")
		f.write("\n")
	f.close()
	update_user_list()

def produce(topic):
	global producer
	inp=input("Enter Message : ")
	inp=current_user+" : "+inp
	print("Sent message to "+inp)
	producer.send(topic, value=inp)

def consume(topic1):
	# print(topic1)
	global login
	consumer = KafkaConsumer(
							    topic1,
								bootstrap_servers=['localhost:9092'],
								auto_offset_reset='earliest',
								enable_auto_commit=True,
								group_id='my-group',
								value_deserializer=lambda x: loads(x.decode('utf-8')))
	fil="chat_"+topic1+".txt"
	file=open(fil,"a+")

	#print("Hello Consumer")
	while True:
		for message in consumer:
			# print("hello")
			# break
			message = message.value
			file.write(str(message)+"\n")
			print(Fore.GREEN+"New Message from : "+Fore.RED,message)
			print(Fore.WHITE)
			breakm n
		if(login==0):
			print("logout")
			file.close()
			break

	print("end")


def update_user_list():
	global users
	global login
	global current_user
	global contacts
	
	f = open("users_list.txt", "r")
	data=f.readlines()
	#print(data)
	users={}
	login=0
	for line in data:
		list_of_users=line.strip().split(" ")
		if(list_of_users!=['']):
			users[list_of_users[0]]=list_of_users[1]
			contacts[list_of_users[0]]=[]
	f.close()

	f = open("users_contact_list.txt", "r")
	data=f.readlines()
	#print(data)
	contacts={}
	for line in data:
		list_of_users=line.strip().split(" ")
		if(list_of_users!=['']):
			contacts[list_of_users[0]]=list_of_users[1:]
	f.close()



def validate_user(username,password):
	global users
	global login
	global current_user
	global contacts
	global producer

	if(not username in users):
		print("******** First login ***********")
	else:
		if(users[username]==password):
			if(login==0):
				login=1
				current_user=username
				print("Successfully logged in")
				producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
				t1 = threading.Thread(target=consume,args=(username,))
				t1.start()
			else:
				print("Already logged in")
		else:
			print("Password is incorrect")

def add_user(username,password):
	f = open("users_list.txt", "a")
	f.write(username+" "+password)
	f.write("\n")
	f.close()
	update_user_list()
	print("Successfully registered")


if __name__=="__main__":
	print("Instant Messaging System : ")
	update_user_list()
	print(users)
	while(0.8):
		print("\n\n")
		sleep(1)
		cmd=input("--->>> ")
		if(cmd=="login"):
			username=input("Enter name : ")
			password=input("Enter password : ")
			validate_user(username,password)
		elif(cmd=="add_user"):
			username=input("Enter name : ")
			password=input("Enter password : ")
			add_user(username,password)
		elif(cmd=="contact_list"):
			if(current_user==""):
				print("******** First login ***********")
			else:
				print(contacts[current_user])
		elif(cmd=="logout"):
			login=0
			print("logged out Successfully")
			produce(current_user)
		elif(cmd=="send"):
			topic=input("Enter topic : ")
			produce(topic)
		elif(cmd=="add_contact"):
			topic=input("Enter topic : ")
			add_contact(topic)
		elif(cmd=="chat_history"):
			topic=input("Enter topic : ")
			add_contact(topic)
		elif(cmd=="exit"):
			break
	print("Bye")

