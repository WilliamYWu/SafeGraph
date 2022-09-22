from global_methods import *

import pyodbc
import mysql.connector as mysql
from mysql.connector import Error

def create_database():
    try:
        conn = mysql.connect(
            host = "localhost", 
            user = "root",
            password = "Sbh123ql46!#"
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS safegraph_db")
    except Exception as e:
        logging.error("Failed to connect to SQL Database.")

def create_raw_table():
    try:
        mydb = mysql.connect(
            host = "localhost", 
            database = "safegraph_db",
            user = "root",
            password = "Sbh123ql46!#"
        )
        cursor = mydb.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute('''CREATE TABLE IF NOT EXISTS raw_data
            (placekey VARCHAR(255),	
            parent_placekey	VARCHAR(255),
            location_name VARCHAR(255),
            safegraph_brand_ids	VARCHAR(255),
            brands VARCHAR(255),
            top_category VARCHAR(255),
            sub_category VARCHAR(255),	
            naics_code VARCHAR(255),	
            latitude FLOAT,	
            longitude FLOAT,	
            street_address VARCHAR(255),
            city VARCHAR(255),	
            region VARCHAR(255),	
            postal_code VARCHAR(255),	
            iso_country_code VARCHAR(255),	
            phone_number VARCHAR(255),	
            open_hours TEXT,	
            category_tags VARCHAR(255),	
            opened_on VARCHAR(255),	
            closed_on VARCHAR(255),	
            tracking_closed_since VARCHAR(255),	
            geometry_type VARCHAR(255),	
            spend_date_range_start DATE,	
            spend_date_range_end DATE,	
            raw_total_spend	FLOAT,
            raw_num_transactions INT,	
            raw_num_customers INT,	
            median_spend_per_transaction FLOAT,	
            median_spend_per_customer FLOAT,	
            spend_per_transaction_percentiles TEXT,	
            spend_by_day TEXT,	
            spend_per_transaction_by_day TEXT,	
            spend_by_day_of_week TEXT,	
            day_counts TEXT,	
            spend_pct_change_vs_prev_month INT,	
            spend_pct_change_vs_prev_year INT,  	
            online_transactions	INT,
            online_spend INT,	
            transaction_intermediary FLOAT,	
            spend_by_transaction_intermediary TEXT,	
            bucketed_customer_frequency	TEXT,
            mean_spend_per_customer_by_frequency TEXT,	
            bucketed_customer_incomes TEXT,	
            mean_spend_per_customer_by_income TEXT,	
            customer_home_city TEXT
            )''')
    except Exception as e:
            logging.error("Failed to connect to SQL Database.")

def from_file_populate_table(file, table):
    print(file)
    mydb = mysql.connect(
        host = "localhost", 
        database = 'safegraph_db',
        user = "root",
        password = "Sbh123ql46!#"
    )
    cursor = mydb.cursor()
    try:
        csv_df = pd.read_csv(file, engine='c', on_bad_lines='skip').fillna('NA')
    except Exception as e:
        logging.error(f'Error on: {file}: {e}')
    csv_df = clean(csv_df)
    sql = f"INSERT IGNORE INTO safegraph_db.{table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" 
    for i, row in csv_df.iterrows():
        cursor.execute(sql, tuple(row))
        mydb.commit()

