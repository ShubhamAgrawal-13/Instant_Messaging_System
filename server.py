from pymongo import MongoClient 
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from colorama import Fore

from sys import argv
from _thread import *
import os


login_dict={}
active_dict={}
msgs_dict={}


from pymongo import MongoClient 
import json
  
try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
except:   
    print("Could not connect to MongoDB") 

db = conn['Msg_db']

login=db['login']
msgs=db['msgs']


def new_user(user,pwd):
	temp_login={}
	temp_login["user"]=user
	temp_login["pwd"]=pwd

	login.insert_one(temp_login)

	temp_msgs={}
	temp_msgs["user"]=user
	temp_msgs["msg"]={}
	temp_msgs["active"]=0

	msgs.insert_one(temp_msgs)


def update_db_active(user,flag):
	global active_dict
	active_dict[user]=flag
	myquery = { "user": user }
	newvalues = { "$set": { "active": flag }  }
	msgs.update_one(myquery, newvalues)



def validate(user,pwd):
	if(user in login_dict):
		if(login_dict[user]==pwd):
			return True
		else:
			return False
	else:
		return False



def initialize():
	global login_dict
	global active_dict

	print(login.count())

	for record in login.find():
		login_dict[record["user"]]=record["pwd"]

	print(login_dict)

	for record in msgs.find():
		active_dict[record["user"]]=record["active"]

	print(active_dict)


def handle_client(conn):
	conn.send(b'connected')	
	msg=conn.recv(1024).decode()
	msg=msg.split(":")
	print(msg)
	flag=validate(msg[0],msg[1])
	if(flag):
		conn.send(str.encode("ok"))
	else:
		conn.send(str.encode("not ok"))
	update_db_active(msg[0],1)


#server code

initialize()

import socket

server="192.168.1.3"
port=5544

s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.bind((server,port))
print("Server binded to port",port)

s.listen()
print("Listening .....")

	
while(1):
	conn, addr = s.accept()
	print("Connected to ",addr)
	start_new_thread(handle_client,(conn,))




