import mysql.connector
import pandas as pd

# Connect to MySQL database
connection = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='1234',
    database='mydb',
    auth_plugin='mysql_native_password'
)

if connection.is_connected():
    print("Connected to the MySQL database")

    # Read CSV file into a pandas DataFrame
    csv_file_path = "C:/Users/jeremp/Documents/New folder (2)/WDIFootNote.csv"
    df = pd.read_csv(csv_file_path, index_col=0)

    # Replace NaN values with a placeholder (e.g., 'NA')
    df = df.fillna('NA')

    # Ensure DataFrame column names match the database table column names
    df.columns = [column_name.lower() for column_name in df.columns]


    # Specify the table name in the database
    table_name = 'footnote'

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Create the INSERT query with placeholders
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"

    

    try:
        # Iterate through rows and insert data into the table
        for index, row in df.iterrows():
            values = tuple(row)
            print("Query:", insert_query % values)           
            print("Values:", values)
            cursor.execute(insert_query, values)

        # Commit the changes to the database
        connection.commit()
        print(f"Data inserted into {table_name} successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that may occur during insertion
        print(f"Error: {err}")
        connection.rollback()

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

else:
    print("Failed to connect to the MySQL database")
