import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()
MARIADB_USER = 'root' #os.environ.get("MARIADB_USER")
MARIADB_PASSWORD = os.environ.get("MARIADB_ROOT_PASSWORD")
MARIADB_DATABASE = os.environ.get("MARIADB_DATABASE")

try:
    connection = mysql.connector.connect(host='mariadb',
                                         database=MARIADB_DATABASE,
                                         user=MARIADB_USER,
                                         password=MARIADB_PASSWORD,
                                         port=3306)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")