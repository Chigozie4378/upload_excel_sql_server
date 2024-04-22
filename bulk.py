import pandas as pd
import pyodbc
import os
 
    
def infer_sql_datatype(data_type):
    if 'int' in str(data_type):
        return 'INT'
    elif 'float' in str(data_type):
        return 'FLOAT'
    elif 'datetime' in str(data_type):
        return 'DATETIME'
    elif 'object' in str(data_type):
        if 'currency' in str(data_type):  # Check for currency data type
            return 'MONEY'  
        else:
            return 'VARCHAR(MAX)'
    else:
        return 'VARCHAR(1000)'


csv_dir = r'C:\Users\ezesu\OneDrive\Desktop\upload_excel_SQL_server\data'

csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=CHIGOZ\SQLEXPRESS;'
                      'DATABASE=upload;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for csv_file in csv_files:
    df = pd.read_csv(os.path.join(csv_dir, csv_file))

    df.columns = df.columns.str.replace(' ', '_')  
    df.columns = df.columns.str.replace('[^\w\s]', '') 
    df.columns = df.columns.str.replace('-', '_') 

    table_name = "[" + os.path.splitext(csv_file)[0] + "]" 
    
    # Create table
    create_table_query = f"CREATE TABLE {table_name} ("
    for column_name, data_type in zip(df.columns, df.dtypes):
        create_table_query += f"[{column_name}] {infer_sql_datatype(data_type)}, "
    create_table_query = create_table_query[:-2] 
    create_table_query += ");"
    cursor.execute(create_table_query)
    
    # Bulk insert
    cursor.fast_executemany = True
    insert_query = f"BULK INSERT {table_name} FROM '{os.path.join(csv_dir, csv_file)}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK)"
    cursor.execute(insert_query)
    conn.commit()

cursor.close()
conn.close()


