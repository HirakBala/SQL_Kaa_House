import time
start_time = time.time()
import pandas as pd
import mysql.connector
from mysql.connector import Error



username = 'root'             # Replace with your MySQL username
password = 'legXXd@07'        # Replace with your MySQL password
host = '127.0.0.1'            # MySQL host (e.g., 'localhost' or '127.0.0.1')
port = '3306'                 # MySQL port (default is 3306)
database = 'bronze'           # Your database name


try:
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )

    if connection.is_connected():
        cursor = connection.cursor()
        print(f"Connected to the database: {database}")

except Error as e:
    print(f"Database connection failed: {e}")
    exit()


csv_table_mapping = {
    r'C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv': 'bronze_cust_info',
    r"C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv": 'bronze_cust_az',
    r"C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv": 'bronze_loc_a',
    r"C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv": 'bronze_sales_info',
    r"C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv": 'bronze_prod_info',
    r"C:\Users\HP\Documents\SQL Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv": 'bronze_prod_cat'
    # Just add more CSV files using your path then the tables name in SQL
    # r'path_to_csv_file.csv': 'table_name_in_sql'
}


def format_dates(df, date_columns):
    for col in date_columns:
        if col in df.columns:
           
            df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace({pd.NaT: None})  # Replace NaT with None for SQL compatibility
    return df


for csv_file, table_name in csv_table_mapping.items():
    try:

        df = pd.read_csv(csv_file)
        
       
        if table_name == 'bronze_cust_info':
            df = format_dates(df, ['cust_createdate'])
        elif table_name == 'bronze_prod_info':
            df = format_dates(df, ['prd_startdate', 'prd_enddate'])
        
       
        columns = ', '.join(df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        
      
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cursor.executemany(sql, data)
        
        connection.commit()
        print(f"Data from {csv_file} successfully ingested into {table_name}!")
    
    except Exception as e:
        print(f"Error ingesting {csv_file} into {table_name}: {e}")


cursor.close()
connection.close()
print("Database connection closed.")

end_time = time.time()

print("Total load time to sql_tables is", end_time - start_time, "seconds.")
