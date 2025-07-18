from fastapi import FastAPI
import pandas as pd
import psycopg2

app = FastAPI()

connection = psycopg2.connect(database="", user="", password="", host="", port=5432)

def execute_query(sql: str):
    try:
        cursor = connection.cursor()
        print(sql)
        cursor.execute(sql)

        # Fetch all rows from database
        record = cursor.fetchall()
        return record
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()

def handle_get_customer(name: str): 
    db_query = f"SELECT * FROM dbt.customer WHERE customer_name = '{name}'"
    data = execute_query(db_query)
    return execute_query(sql)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/customers')
async def get_customers(name: str):
    return handle_get_customer()
