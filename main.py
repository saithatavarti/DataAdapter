import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine
from fastapi import FastAPI, Query

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


#  dynamic method for opting a yaml file 

@app.get("/postgresql/{choose_id}/{username}/{minimum}/{offset}")
async def get_component(choose_id,username,minimum,offset):
	url = choose_id
	us = username
	lower= int(minimum)
	upper= int(offset)
	min = int(offset)-int(minimum)+1
	max = int(minimum)-1

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
	page =  int(documents["query"]["page_size"])

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
	
	query1="select "	
	if (columns=="None"):
		return("column names incorrect")
	else:
		query1=query1+"count("+columns+") "
	if (table=="None"):
		return("table doesn't exist")
	else:
		query1=query1+"from"+" "+table
	if (where== "None"):
		query1=query1
	else:
		query1=query1+" "+"where"+" "+where
	if(group=="None"):
		query1=query1
	else:
		query1=query1+" "+"group by"+" "+group
	if(order=="None"):
		query1=query1
	else:
		query1=query1+" "+"order by"+" "+order
	print(query1)


	if(limit=="None"):
		max=max
	elif(int(limit)<=int(offset)):
		offset=int(limit)
		min = int(offset)-int(minimum)+1
		max = int(minimum)-1
	else:
		max=max
	if(max<0 or min<1):
		return("limit is given wrong")
	else:
		query=query+" "+"limit"+" "+str(min)+" offset "+str(max)



	# db connection 
	
		engine = create_engine(link,echo=False)
		connection=engine.connect()
		userQuery = "select * from username where name = '{0}' ".format(us)
		print (userQuery)
		usercheck = connection.execute(userQuery).fetchall()
		print(usercheck)


	if usercheck:
		lst = connection.execute(query).fetchall()
		lst1 = connection.execute(query1).fetchall()
		f=int(lst1[0][0])
		if(f%int(page)==0):
			total_pages=int(f/page)
		else:
			total_pages=int((f)/page)+1
		
		str1="next_url== http://127.0.0.1:8000/postgresql/"+url+"/"+us+"/"+str(lower+page)+"/"+str(upper+page)
		print(min,max)


		if((upper/10)>total_pages):
			return("end of pages")
		else:
			return lst,total_pages,str1
				# print(lst)
				# return(lst)
	else:
		return "User Does not Exists"
   


   