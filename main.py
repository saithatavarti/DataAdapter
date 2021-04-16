# postgresql 

import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine
from fastapi import FastAPI, Query

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/{choose_id}/{username}/{minimum}/{offset}")
async def get_component(choose_id,username,minimum,offset):
	url = choose_id
	us = username
	lower= int(minimum)
	upper= int(offset)
	min = int(offset)-int(minimum)+1
	max = int(minimum)-1
	page = int(upper-lower+1)
	
	with open(url+".yaml") as file:
		documents = yaml.safe_load(file)
	adapter = str(documents["parameters"]["adapter"])    
	username = str(documents["parameters"]["username"])
	password = str(documents["parameters"]["password"])
	db = documents["parameters"]["database"]
	port = str(documents["parameters"]["database_port"])
	address = str(documents["parameters"]["address"])
	query = str(documents["query"]["code"])
	link =""
	link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db

	if(max<0 or min<1):
		return("limit is given wrong")
	else:
		query1=query+" "+"limit"+" "+str(min)+" offset "+str(max)

	# db connection 
	
	engine = create_engine(link,echo=False)
	connection=engine.connect()
	userQuery = "select * from username where name = '{0}' ".format(us)
	print (userQuery)
	usercheck = connection.execute(userQuery).fetchall()
	print(usercheck)

	if usercheck:
		
		lst = connection.execute(query1).fetchall()
		count= len(connection.execute(query).fetchall())
		f=int(count)
		if(f%int(page)==0):
			total_pages=int(f/page)
		else:
			total_pages=int((f)/page)+1
		
		str1="next_url== "+address+url+"/"+us+"/"+str(lower+page)+"/"+str(upper+page)
		print(min,max)
		data= "data: "
		meta= "meta: "

		if((upper/page)>total_pages):
			return("end of pages")
		else:
			return data,lst,{meta ,total_pages,str1}
	else:
		return "User Does not Exists"
   


   