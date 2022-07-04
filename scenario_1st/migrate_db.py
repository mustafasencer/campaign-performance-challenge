# DROP TABLES-----------------------------------------------------------------------------------------------------------
fact_table_table_drop = "DROP TABLE  IF EXISTS fact_table"
user_table_drop = "DROP TABLE IF EXISTS  users"
customer_table_drop = "DROP TABLE IF EXISTS  customers"
event_table_drop = "DROP TABLE IF EXISTS  events"
event_type_table_drop = "DROP TABLE  IF EXISTS event_types"
time_table_drop = "DROP TABLE  IF EXISTS time"

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

fact_table_unique_index = """
CREATE UNIQUE index idx_fact_table_date_hour_customer_id ON fact_table (date_hour, customer_id)
"""

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

event_type_table_create = """CREATE TABLE  IF NOT EXISTS event_types(
	event_type_id INT CONSTRAINT event_types_pk PRIMARY KEY,
	event_type VARCHAR
)"""

time_table_create = """CREATE TABLE IF NOT EXISTS  time(
	fired_at  TIMESTAMP CONSTRAINT time_pk PRIMARY KEY,
	hour INT NOT NULL CHECK (hour >= 0),
	day INT NOT NULL CHECK (day >= 0),
	week INT NOT NULL CHECK (week >= 0),
	month INT NOT NULL CHECK (month >= 0),
	year INT NOT NULL CHECK (year >= 0),
	weekday VARCHAR NOT NULL
)"""

# INSERT RECORDS--------------------------------------------------------------------------------------------------------
fact_table_table_insert = """INSERT INTO fact_table (date_hour, customer_id, page_loads, clicks, unique_user_clicks, click_through_rate)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (date_hour, customer_id) DO NOTHING
"""

# Inserting the user on conflict do nothing
user_table_insert = """INSERT INTO users (user_id, email, ip) VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING
"""

# Inserting the customer on conflict do nothing
customer_table_insert = """INSERT INTO customers (customer_id) VALUES (%s)
                        ON CONFLICT (customer_id) DO NOTHING
"""

# Inserting the event on conflict do nothing
event_table_insert = """INSERT INTO events (event_id, customer_id, user_id, fired_at, event_type) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
"""

# Inserting the event type on conflict do nothing
event_type_table_insert = """INSERT INTO event_types (event_type_id, event_type) VALUES (DEFAULT, %s)
                          ON CONFLICT (event_type_id) DO NOTHING
"""

# Inserting the time on conflict do nothing
time_table_insert = """INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (fired_at) DO NOTHING
"""

# QUERY LISTS-----------------------------------------------------------------------------------------------------------
create_table_queries = [
    user_table_create,
    customer_table_create,
    event_table_create,
    fact_table_table_create,
    fact_table_unique_index,
]
drop_table_queries = [
    fact_table_table_drop,
    customer_table_drop,
    user_table_drop,
    event_table_drop,
]
