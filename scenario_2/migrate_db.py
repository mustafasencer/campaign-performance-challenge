# DROP TABLES-----------------------------------------------------------------------------------------------------------
fact_table_table_drop = "DROP TABLE  IF EXISTS fact_table"
user_table_drop = "DROP TABLE IF EXISTS  users"
customer_table_drop = "DROP TABLE IF EXISTS  customers"
event_table_drop = "DROP TABLE IF EXISTS  events"

# CREATE TABLES---------------------------------------------------------------------------------------------------------
fact_table_table_create = """CREATE TABLE IF NOT EXISTS fact_table(
	fact_table_id SERIAL CONSTRAINT fact_table_pk PRIMARY KEY,
	date_hour timestamp NOT NULL,
	customer_id INT REFERENCES customers (customer_id),
	page_loads INT,
	clicks INT,
	unique_user_clicks INT,
	click_through_rate FLOAT
)"""

user_table_create = """CREATE TABLE IF NOT EXISTS  users(
	user_id  INT CONSTRAINT users_pk PRIMARY KEY,
	email  VARCHAR,
	ip  VARCHAR
)"""

customer_table_create = """CREATE TABLE IF NOT EXISTS  customers(
	customer_id  INT CONSTRAINT customers_pk PRIMARY KEY
)"""

event_table_create = """CREATE TABLE  IF NOT EXISTS events(
	event_id VARCHAR CONSTRAINT events_pk PRIMARY KEY,
	customer_id INT REFERENCES customers (customer_id),
	fired_at TIMESTAMP,
	user_id INT REFERENCES users (user_id),
	event_type VARCHAR
)"""

# INSERT RECORDS--------------------------------------------------------------------------------------------------------
fact_table_table_insert = """INSERT INTO fact_table (date_hour, customer_id, page_loads, clicks, unique_user_clicks, click_through_rate)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (date_hour, customer_id) DO NOTHING
"""

# Updating the user level on conflict
user_table_insert = """INSERT INTO users (user_id, email, ip) VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING
"""

# Updating the user level on conflict
customer_table_insert = """INSERT INTO customers (customer_id) VALUES (%s)
                        ON CONFLICT (customer_id) DO NOTHING
"""

event_table_insert = """INSERT INTO events (event_id, customer_id, user_id, fired_at, event_type) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
"""

# SELECT events---------------------------------------------------------------------------------------------------------

# QUERY LISTS-----------------------------------------------------------------------------------------------------------
create_table_queries = [
    user_table_create,
    customer_table_create,
    event_table_create,
    fact_table_table_create,
]
drop_table_queries = [
    fact_table_table_drop,
    customer_table_drop,
    user_table_drop,
    event_table_drop,
]
