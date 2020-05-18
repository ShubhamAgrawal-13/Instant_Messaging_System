# import mysql.connector as conn

# mydb = conn.connect(host="localhost", user="root", passwd="root", database="msgdb")
# #print(mydb)

# if(mydb):
# 	print("Connected")
# else:
# 	print("Not able to connect")

# mycursor = mydb.cursor()

# # mycursor.execute("Create database msgdb")
# # mycursor.execute("Show databases")

# # for db in mycursor:
# # 	print(db)

# mycursor.execute("create")


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

login_data=[
    {
        "user":"user1",
        "pwd":"1"
    },
    {
        "user":"user2",
        "pwd":"2"
    },
    {
        "user":"user3",
        "pwd":"3"
    }
]

msgs_data=[
    {
        "user":"user1",
        "msg":{},
        "active":0
    },
    {
        "user":"user2",
        "msg":{},
        "active":0
    },
    {
        "user":"user3",
        "msg":{},
        "active":0
    }
]

# login.insert_many(login_data)
msgs.insert_many(msgs_data)

# for c in msgs.find():
# 	print(c)



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
	myquery = { "user": user }
	newvalues = { "$set": { "active": flag }  }
	msgs.update_one(myquery, newvalues)

def update_db_msg(user,msg):
	myquery = { "user": user }
	newvalues = { "$set": { "msg": msg }  }
	msgs.update_one(myquery, newvalues)




