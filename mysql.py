
import yaml
from fastapi import FastAPI,Response
app = FastAPI()

@app.get("/{user_name}/{file_name}/{min_limit}/{max_limit}")
def fun(user_name,file_name,min_limit,max_limit):
	f=file_name
	user=user_name
	lower=int(min_limit)
	upper=int(max_limit)
	min=int(min_limit)-1
	max=int(max_limit)-int(min)
	import yaml
	with open(f+'.yaml') as file:
		documents = yaml.safe_load(file)
	adapter=str(documents["development"]["adapter"])
	username=str(documents["development"]["username"])
	password=str(documents["development"]["password"])
	query=str(documents["query"]["code"])
	dbs=str(documents["development"]["database"])
	page=max
	url=str(documents["development"]["url"])
	link=adapter+"://"+username+":"+password+"@localhost/"+dbs
	
	if(max<0 or min<0):
		return("limit is given wrong")
	else:
		query1=query+" "+"limit"+" "+str(min)+","+str(max)
	
	import json
	from sqlalchemy import create_engine
	engine = create_engine(link)
	connection=engine.connect()
	output = connection.execute(query1).fetchall()
	count = len(connection.execute(query).fetchall())
	
	count=int(count)
	if(count%int(page)==0):
		total_pages=int(count/page)
	else:
		total_pages=int(count/page)+1
	
	url="next_url:"+url+user+"/"+f+"/"+str(lower+page)+"/"+str(upper+page)
	print(min,max)
	data="data:"
	meta="meta:"
	end="end of pages"

	page_number="page_num:" + str(int(upper/page))
	if((upper/page)>total_pages):
		return("end of pages")
	elif(int(upper/page)==total_pages):
		return data,output,meta,end,page_number
	else:
		pages="total_pages: "+str(total_pages)
		return data,output,meta,url,page_number,pages

	
    
