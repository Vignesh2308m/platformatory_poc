import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import os

# Function to generate fake product details
def generate_fake_product():
    fake = Faker()
    created = fake.date_time_this_decade()
    modified = created + timedelta(days=fake.random_int(min=1, max=365))
    name = fake.company()
    category = fake.word()
    price = round(fake.random.uniform(1, 1000), 2)
    return created, modified, name, category, price

# Function to insert fake product details into the PostgreSQL table
def insert_fake_products(conn, cursor, num_products=100):
    for _ in range(num_products):
        created, modified, name, category, price = generate_fake_product()
        query = """
            INSERT INTO public.product ("created", "modified", name, category, price)
            VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(query, (created, modified, name, category, price))
        conn.commit()

# Connect to PostgreSQL database
db_params = {
    "database": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT")
}

conn = psycopg2.connect(**db_params)
# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Call the function to insert fake products
insert_fake_products(conn, cursor, num_products=100)

# Close the cursor and connection
cursor.close()
conn.close()
