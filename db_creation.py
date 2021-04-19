import sqlite3

#Creating database
con = sqlite3.connect("medusa.db") 
print("Database opened successfully")

#Creating table
con.execute("create table students_sqlite (id INTEGER PRIMARY KEY AUTOINCREMENT,email TEXT NOT NULL, name TEXT NOT NULL)")
print("Table created succesfully")