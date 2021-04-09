from sqlalchemy import create_engine,Column,Integer,String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from fastapi_pagination import Page, add_pagination, paginate
from fastapi.responses import JSONResponse
import json
import yaml
import json
from sqlalchemy import create_engine
from fastapi import FastAPI
app = FastAPI()
@app.get("/")
def main_page():
    return {"Hello": "World"}
@app.get("/oracledata/{choose_id}/{b}/{c}") 
async def get_component(choose_id,b,c):
	a = choose_id
	d=b
	e=c
	with open(a+".yaml") as file:
		documents = yaml.safe_load(file)
	adapter=str(documents["development"]["adapter"])    
	username=str(documents["development"]["username"])
	password=str(documents["development"]["password"])
	pagesize=str(documents["development"]["pagesize"])
	dbs=documents["development"]["database"]
	query=str(documents["query"])
	query2=str(documents['query2'])
	print('the output of query2 is {}'.format(query2))
	vb=query2.split()
	if('group' in vb):
		query1=query2.replace('*','count(count(*))')
	else:
		query1=query2.replace("*","count(*)")
	print(query1)
	link=adapter+"://"+username+":"+password+"@localhost:1521/"+dbs
	engine = create_engine(link)
	connection=engine.connect()
	total_count= connection.execute(query1).fetchall()
	print(query1)
	print(query)
	if (('where' in vb) or ('group by' in vb)):
		# query='select * from employee'
		query='select * from ({}) where rownum<={} minus select * from ({}) where rownum<={}'.format(query2,e,query2,d)
	else:
		# query='select * from employee'
		query=query2+' where rownum<={} minus {} where rownum<={}'.format(e,query2,d)
	print('the query string is {}'.format(query))
	print('the value of e is {} and total colunt is {} '.format(e,total_count[0][0]))
	if (int(e)==int(total_count[0][0])):
		nxt="end of pages "
		print("hello")
	elif((int(e)+(int(e)-int(d)))<=total_count[0][0]):
		nxt="/oracledata/{}/{}/{}".format(a,e,int(e)+(int(e)-int(d)))
	else:
		nxt="/oracledata/{}/{}/{}".format(a,e,total_count[0][0])
	lst = connection.execute(query).fetchall()
	print(total_count)
	print(lst)
	return "link to next page is {} data  and data is {} and totla count is {}".format(nxt,lst,total_count[0][0])
	

