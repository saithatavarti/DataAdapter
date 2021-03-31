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

@app.get("/postgresql/{choose_id}")
async def get_component(choose_id):
	a = {"file": choose_id}
	b = (a["file"])
	with open(b+".yaml") as file:
		documents = yaml.safe_load(file)
	adapter = str(documents["parameters"]["adapter"])    
	username = str(documents["parameters"]["username"])
	password = str(documents["parameters"]["password"])
	db = documents["parameters"]["database"]
	port = str(documents["parameters"]["database_port"])

	# query
	column = str(documents["query"]["column"])
	table = str(documents["query"]["table"])
	limit = str(documents["query"]["limit"])

	# apended variable
	link =""
	link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db
	query = "select"+" "+column+" "+"from"+" "+table +" " +"LIMIT"+ " " +limit

	# db connection 
	engine = create_engine(link,echo=False)
	connection=engine.connect()
	lst = connection.execute(query).fetchall()
	return(lst)    
   


   