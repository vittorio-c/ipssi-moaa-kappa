import mysql.connector

mariabdb_client = mysql.connector.connect(
  host="mariadb",
  user="lambda-user",
  password="user_password",
  database="moaa_db"
)
