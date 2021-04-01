import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine
from typing import List, Optional
from fastapi import FastAPI, Query

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# @app.get("/items")
# def test():
# 	with open(r'test.yaml') as file:
# 		documents = yaml.safe_load(file)
# 	adapter = str(documents["parameters"]["adapter"])    
# 	username = str(documents["parameters"]["username"])
# 	password = str(documents["parameters"]["password"])
# 	query = str(documents["query"])
# 	db = documents["parameters"]["database"]
# 	port = str(documents["parameters"]["database_port"])
# 	link =""
# 	link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db
# 	# db connection 
# 	engine = create_engine(link,echo=False)
# 	connection=engine.connect()
# 	lst = connection.execute(query).fetchall()
# 	return(lst)


#  dynamic method for opting a yaml file 

@app.get("/postgresql/{choose_id}/{username}")
async def get_component(choose_id,username):
	url = choose_id
	us = username

	with open(url+".yaml") as file:
		documents = yaml.safe_load(file)
	adapter = str(documents["parameters"]["adapter"])    
	username = str(documents["parameters"]["username"])
	password = str(documents["parameters"]["password"])
	db = documents["parameters"]["database"]
	port = str(documents["parameters"]["database_port"])

	# query
	columns = str(documents["query"]["column"])
	table = str(documents["query"]["table"])
	limit = str(documents["query"]["limit"])
	where = str(documents["query"]["where"])
	order = str(documents["query"]["order"])
	group = str(documents["query"]["group"])
	

	# apended variable
	link =""
	link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db
	# query = "select"+" "+column+" "+"from"+" "+table +" " +"LIMIT"+ " " +limit
	query="select "	
	if (columns=="None"):
		return("column names incorrect")
	else:
		query=query+columns+" "
	if (table=="None"):
		return("table doesn't exist")
	else:
		query=query+"from"+" "+table
	if (where== "None"):
		query=query
	else:
		query=query+" "+"where"+" "+where
	if(group=="None"):
		query=query
	else:
		query=query+" "+"group by"+" "+group
	if(order=="None"):
		query=query
	else:
		query=query+" "+"order by"+" "+order
	if (limit=="None"):
		query=query
	else:
		query=query+" "+"limit"+" "+limit

	# db connection 
	
		engine = create_engine(link,echo=False)
		connection=engine.connect()
		userQuery = "select * from username where name = '{0}' ".format(us)
		print (userQuery)
		usercheck = connection.execute(userQuery).fetchall()
		print(usercheck)


	if usercheck:
		lst = connection.execute(query).fetchall()
		print(lst)
		return(lst)
	else:
		return "User Does not Exists"
   


   