import sqlite3

def connect_to_db(db_name: str):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        print(f"Connected to {db_name} successfully!!")
        return conn, cursor
    except sqlite3.Error as error:
        print(f"Failed to connect to {db_name}, error: {error}")

def create_table(cursor : sqlite3.Cursor,
                 conn : sqlite3.Connection,
                 table_name: str,
                 columns: dict[str,str]):
    try:
        column_content = ""
        for column_name , dtype in columns.items():
            column_content += column_name + " " + dtype + ", "
        column_content = column_content.rstrip(", ")

        query = f"CREATE TABLE {table_name} ({column_content});"

        cursor.execute(query)
        conn.commit()
        print(f"Table {table_name} was created successfully!!")
    except sqlite3.Error as error:
        print(f"Failed to create table {table_name}, error: {error}")

def insert_data(cursor , conn , table_name , data):
    placeholders = "?,?,?,?"
    try:
        placeholders = ", ".join(["?"] * len(data[0]))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(query , data)
        conn.commit()
        print(f"Inserted {len(data)} records into {table_name} successfully!!")
    except sqlite3.Error as error:
        print(f"Failed to insert into {table_name}, error: {error}")

def fetch_data(cursor , table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchall()
        print(f"All data from {table_name} was fetched successfully!!")
        for row in result:
            print(row)
    except sqlite3.Error as error:
        print(f"Failed to fetch data from {table_name}, error: {error}")

def fetch_data_by_condition(cursor , table_name , condition: str):
    try:
        query = f"SELECT * FROM {table_name} WHERE {condition};"
        cursor.execute(query)
        result = cursor.fetchall()
        print(f"All data from {table_name} was fetched successfully!! \nWHERE {condition} was fetched successfully!!")
        for row in result:
            print(row)
    except sqlite3.Error as error:
        print(f"Failed to fetch data from {table_name}, error: {error}")

def update_data(cursor , conn , table_name , updates , condition):
    try:
        query = f"UPDATE {table_name} SET {updates} WHERE {condition};"
        cursor.execute(query)
        conn.commit()
        print(f"Updated {table_name} successfully!!")
    except sqlite3.Error as error:
        print(f"Failed to update {table_name}, error: {error}")

def delete_data(cursor , conn , table_name , condition):
    try:
        query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(query)
        conn.commit()
        print(f"Deleted by condition row on the table: {table_name} successfully!!")
    except sqlite3.Error as error:
        print(f"Failed to delete {table_name}, error: {error}")

def close_connection(conn):
    try:
        conn.close()
        print(f"Connection closed successfully!!")
    except sqlite3.Error as error:
        print(f"Failed to close connection, error: {error}")


conn , cursor = connect_to_db("bda2501_group.db")

columns_to_table = {
    "id" : "INTEGER PRIMARY KEY AUTOINCREMENT",
    "name" : "TEXT",
    "age" : "INTEGER",
    "grade" : "REAL",
    "city" : "TEXT",
    "country" : "TEXT"
}
create_table(cursor , conn , "bda2501_group", columns_to_table)

info_for_table = [
    [1,"Raiymbek",18,100,"Shymkent","Kazakhstan"],
    [2,"Alimzhan",17,95,"Pavlodar","Russia"],
    [3,"Azat",20,90,"Petropavlovsk","England"],
    [4,"Arsen",33,99,"Astana","USA"]
]
insert_data(cursor , conn , "bda2501_group" , info_for_table)

fetch_data(cursor , "bda2501_group")

fetch_data_by_condition(cursor , "bda2501_group" , "country = 'USA'")

update_data(cursor , conn , "bda2501_group" , "grade = 1000" , "id = 3" )
update_data(cursor , conn , "bda2501_group" , "city = 'Taraz'" , "id = 2")
fetch_data(cursor , "bda2501_group")

delete_data(cursor , conn , "bda2501_group" , "country = 'Russia'")
fetch_data(cursor , "bda2501_group")

close_connection(conn)