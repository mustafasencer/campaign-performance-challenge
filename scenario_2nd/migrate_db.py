# DROP TABLES-----------------------------------------------------------------------------------------------------------
fact_table_table_drop = "DROP TABLE  IF EXISTS fact_table"
unique_user_table_drop = "DROP TABLE  IF EXISTS unique_user"
user_table_drop = "DROP TABLE IF EXISTS  users"
customer_table_drop = "DROP TABLE IF EXISTS  customers"
event_table_drop = "DROP TABLE IF EXISTS  events"

# CREATE TABLES---------------------------------------------------------------------------------------------------------
fact_table_table_create = """CREATE TABLE IF NOT EXISTS fact_table(
	fact_table_id SERIAL CONSTRAINT fact_table_pk PRIMARY KEY,
	date_hour timestamp NOT NULL,
	customer_id INT REFERENCES customers (customer_id),
	page_loads INT DEFAULT 0,
	clicks INT DEFAULT 0,
	unique_user_clicks INT DEFAULT 0,
	click_through_rate INT DEFAULT 0
)"""

fact_table_unique_index = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_table_date_hour_customer_id ON fact_table (date_hour, customer_id)
"""

unique_user_table_create = """CREATE TABLE IF NOT EXISTS unique_user(
	unique_user_id SERIAL CONSTRAINT unique_user_pk PRIMARY KEY,
	date_hour timestamp NOT NULL,
	customer_id INT REFERENCES customers (customer_id),
	user_id INT REFERENCES users (user_id)
)"""

unique_user_unique_index = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_user_date_hour_customer_id_user_id ON unique_user (date_hour, customer_id, user_id)
"""

unique_user_select_index = """
CREATE INDEX IF NOT EXISTS idx_unique_user_date_hour_customer_id ON unique_user (date_hour, customer_id)
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
	prev_fired_at TIMESTAMP,
	user_id INT REFERENCES users (user_id),
	event_type VARCHAR,
	prev_event_type VARCHAR
)"""

# INSERT RECORDS--------------------------------------------------------------------------------------------------------
fact_table_table_page_loads_upsert = """INSERT INTO fact_table (date_hour, customer_id, page_loads)
                                VALUES (%(date_hour)s, %(customer_id)s, %(page_loads)s)
                                ON CONFLICT (date_hour, customer_id) DO UPDATE
                                SET page_loads = %(page_loads)s
"""

fact_table_table_clicks_upsert = """INSERT INTO fact_table (date_hour, customer_id, clicks, unique_user_clicks, click_through_rate)
                                VALUES (%(date_hour)s, %(customer_id)s, %(clicks)s, %(unique_user_clicks)s, %(click_through_rate)s)
                                ON CONFLICT (date_hour, customer_id) DO UPDATE
                                SET clicks = %(clicks)s,
                                unique_user_clicks = %(unique_user_clicks)s,
                                click_through_rate = %(click_through_rate)s
"""

unique_user_table_insert = """INSERT INTO unique_user (date_hour, customer_id, user_id)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (date_hour, customer_id, user_id) DO NOTHING
"""

# Updating the user level on conflict
user_table_insert = """INSERT INTO users (user_id, email, ip) VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING
"""

# Updating the user level on conflict
customer_table_insert = """INSERT INTO customers (customer_id) VALUES (%s)
                            ON CONFLICT (customer_id) DO NOTHING
"""

event_table_upsert = """INSERT INTO events (event_id, customer_id, user_id, fired_at, event_type) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO UPDATE SET
                        event_type = EXCLUDED.event_type,
                        fired_at = EXCLUDED.fired_at,
                        prev_event_type = events.event_type,
                        prev_fired_at = events.fired_at
                        RETURNING
                            event_id,
                            fired_at,
                            event_type,
                            prev_fired_at,
                            prev_event_type
"""

# SELECT events---------------------------------------------------------------------------------------------------------
fact_table_table_select = """SELECT date_hour, customer_id, page_loads, clicks, unique_user_clicks, click_through_rate FROM fact_table
                                WHERE date_hour = %s
                                AND customer_id = %s
"""

unique_user_table_select = """SELECT count(user_id) as count FROM unique_user
                                WHERE date_hour = %s
                                AND customer_id = %s
"""

event_table_select = """SELECT event_id FROM events
                                WHERE customer_id = %s
                                AND user_id = %s
                                AND event_type = %s
                                AND fired_at >= %s
"""

# QUERY LISTS-----------------------------------------------------------------------------------------------------------
create_table_queries = [
    user_table_create,
    customer_table_create,
    event_table_create,
    fact_table_table_create,
    unique_user_table_create,
    fact_table_unique_index,
    unique_user_unique_index,
    unique_user_select_index,
]
drop_table_queries = [
    fact_table_table_drop,
    customer_table_drop,
    user_table_drop,
    event_table_drop,
    unique_user_table_drop,
]
