from global_methods import *
from global_constants import *

import pandas as pd
import pymysql
from urllib.parse import quote
import sqlalchemy

sql_engine = sqlalchemy.create_engine('mysql+pymysql://root:%s@localhost:3306/safegraph_db' % quote('Sbh123ql46!#'))
mydb = sql_engine.connect()

if __name__ == '__main__':
    everything_sql = "SELECT * FROM safegraph_db.raw_data "
    naics_sql = "SELECT DISTINCT(Placekey) FROM safegraph_db.raw_data"
    df = pd.read_sql_query(naics_sql, mydb)

    # Confirmed that they do produce the same exact output
    everything_df = pd.read_sql_query(everything_sql, mydb)
    list = len(everything_df['placekey'].unique())
    print("done")

    