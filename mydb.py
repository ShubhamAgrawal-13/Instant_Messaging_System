import mysql.connector as conn

mydb = conn.connect(host="localhost", user="root", passwd="root")
#print(mydb)

if(mydb):
	print("Connected")
else:
	print("Not able to connect")

mycursor = mydb.cursor()

# mycursor.execute("Create database msgdb")
mycursor.execute("Show databases")

for db in mycursor:
	print(db)

