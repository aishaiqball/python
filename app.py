from fastapi import FastAPI
import pandas as pd
import psycopg2
from pydantic import BaseModel

app = FastAPI()

connection = psycopg2.connect(database="postgres", user="postgres.koaaobwqrcswkcgqpyfl", password="mypasssword12!!", host="aws-0-eu-west-2.pooler.supabase.com", port=5432)

class CustomerCreate(BaseModel):
    customer_name: str
    email: str
    phone_number: str
    address_line_1: str
    city: str

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
    db_query = f"SELECT * FROM shop.customer WHERE customer_name ilike '{name}%'"
    data = execute_query(db_query)
    return data

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/products")
def get_all_products():
    db_query = f"SELECT * FROM shop.product"
    data = execute_query(db_query)
    return data

@app.get("/products/{product_id}")
def get_product(product_id: int):
    db_query = f"SELECT * FROM shop.product WHERE product_id = {product_id}"
    data = execute_query(db_query)
    return data

@app.get("/customers")
def get_customers():
    db_query = f"SELECT customer_id, customer_name, email, city FROM shop.customer"
    data = execute_query(db_query)
    return [
        {
            "customer_id": row[0],
            "customer_name": row[1],
            "email": row[2],
            "city": row[3]
        }
        for row in data
    ]

@app.get("/customers/{customer_id}")
def get_customer(customer_id: int):
    query = f"SELECT * FROM shop.customer WHERE customer_id = {customer_id}"
    data = execute_query(query)
    row = data[0]
    return {
        "customer_id": row[0],
        "customer_name": row[1],
        "email": row[2],
        "phone_number": row[3],
        "address_line_1": row[4],
        "city": row[5]
    }

@app.post("/customers")
def create_customer(customer: CustomerCreate):
    query = """
    INSERT INTO shop.customer (customer_name, email, phone_number, address_line_1, city)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING customer_id
    """
    values = (
        customer.customer_name,
        customer.email,
        customer.phone_number,
        customer.address_line_1,
        customer.city
    )
    cursor = connection.cursor()
    cursor.execute(query, values)
    new_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    return {
        "customer_id": new_id,
        "customer_name": customer.customer_name,
        "email": customer.email,
        "phone_number": customer.phone_number,
        "address_line_1": customer.address_line_1,
        "city": customer.city
    }

@app.put("/customers/{customer_id}")
def update_customer(customer_id: int, customer: CustomerCreate):
    query = """
    UPDATE shop.customer
    SET customer_name = %s,
        email = %s,
        phone_number = %s,
        address_line_1 = %s,
        city = %s
    WHERE customer_id = %s
    RETURNING customer_id
    """
    values = (
        customer.customer_name,
        customer.email,
        customer.phone_number,
        customer.address_line_1,
        customer.city,
        customer_id
    )
    cursor = connection.cursor()
    cursor.execute(query, values)
    updated = cursor.fetchone()
    connection.commit()
    cursor.close()

    return {
        "customer_id": customer_id,
        "customer_name": customer.customer_name,
        "email": customer.email,
        "phone_number": customer.phone_number,
        "address_line_1": customer.address_line_1,
        "city": customer.city
    }
    
@app.get("/orders/{order_id}")
def get_order_details(order_id: int):
    query = """
        SELECT o.order_id,
               o.order_date,
               o.total_amount,
               c.customer_name,
               c.email,
               s.status_name
        FROM shop.orders o
        JOIN shop.customer c ON o.customer_id = c.customer_id
        JOIN shop.order_status s ON o.order_status_id = s.order_status_id
        WHERE o.order_id = %s
    """
    cursor = connection.cursor()
    cursor.execute(query, (order_id,))
    result = cursor.fetchone()
    cursor.close()

    return {
        "order_id": result[0],
        "order_date": result[1],
        "total_amount": result[2],
        "customer_name": result[3],
        "customer_email": result[4],
        "order_status": result[5]
    }

@app.get("/orders/{order_id}/items")
def get_order_items(order_id: int):
    query = """
        SELECT ol.quantity,
               p.product_name,
               p.selling_price,
               (ol.quantity * p.selling_price) AS line_total
        FROM shop.order_line ol
        JOIN shop.product p ON ol.product_id = p.product_id
        WHERE ol.order_id = %s
    """
    cursor = connection.cursor()
    cursor.execute(query, (order_id,))
    results = cursor.fetchall()
    cursor.close()

    items = []
    for row in results:
        items.append({
            "quantity": row[0],
            "product_name": row[1],
            "selling_price": row[2],
            "line_total": row[3]
        })

    return {"order_id": order_id, "items": items}

@app.get("/customers/{customer_id}/orders")
def get_customer_orders(customer_id: int):
    query = """
        SELECT o.order_id,
               o.order_date,
               o.total_amount,
               s.status_name
        FROM shop.orders o
        JOIN shop.order_status s ON o.order_status_id = s.order_status_id
        WHERE o.customer_id = %s
        ORDER BY o.order_date DESC
    """
    cursor = connection.cursor()
    cursor.execute(query, (customer_id,))
    results = cursor.fetchall()
    cursor.close()

    return [
        {
            "order_id": row[0],
            "order_date": row[1],
            "total_amount": row[2],
            "status_name": row[3]
        }
        for row in results
    ]
@app.put("/orders/{order_id}/status")
def update_order_status(order_id: int, new_status_id: int):
    update_query = "UPDATE shop.orders SET order_status_id = %s WHERE order_id = %s"
    cursor = connection.cursor()
    cursor.execute(update_query, (new_status_id, order_id))
    connection.commit()

    select_query = """
        SELECT o.order_id, o.order_date, o.total_amount, s.status_name
        FROM shop.orders o
        JOIN shop.order_status s ON o.order_status_id = s.order_status_id
        WHERE o.order_id = %s
    """
    cursor.execute(select_query, (order_id,))
    result = cursor.fetchone()
    cursor.close()

    return {
        "order_id": result[0],
        "order_date": result[1],
        "total_amount": result[2],
        "status_name": result[3]
    }

@app.delete("/orders/{order_id}")
def delete_order(order_id: int):
    delete_items_query = "DELETE FROM shop.order_line WHERE order_id = %s"
    cursor = connection.cursor()
    cursor.execute(delete_items_query, (order_id,))

    delete_order_query = "DELETE FROM shop.orders WHERE order_id = %s"
    cursor.execute(delete_order_query, (order_id,))

    connection.commit()
    cursor.close()

    return {"message": f"Order {order_id} and its items were successfully deleted."}
