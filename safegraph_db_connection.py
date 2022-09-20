import pyodbc
import mysql.connector
from mysql.connector import Error
import pandas as pd

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "Sbh123ql46!#"
)

mycursor = mydb.cursor()
