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
        return 'VARCHAR(MAX)' 
    else:
        return 'VARCHAR(1000)' 


csv_dir = 'data/'

csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=CHIGOZ\SQLEXPRESS;'
                      'DATABASE=sample;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for excel_file in csv_files:
    df = pd.read_csv(os.path.join(csv_dir, excel_file))

    df.columns = df.columns.str.replace(' ', '_')  
    df.columns = df.columns.str.replace('[^\w\s]', '') 
    df.columns = df.columns.str.replace('-', '_')  # Replace hyphen with underscore

    column_data_types = {col: infer_sql_datatype(df[col].dtype) for col in df.columns}

    table_name = os.path.splitext(excel_file)[0] 
    create_table_query = f"CREATE TABLE {table_name} ("

    for column_name, data_type in column_data_types.items():
        create_table_query += f"[{column_name}] {data_type}, "
        print(f"Added column: {column_name} ({data_type})")

    
    create_table_query = create_table_query[:-2]
    create_table_query += ");"

    print("CREATE TABLE query:", create_table_query) 

    cursor.execute(create_table_query)
        
    for index, row in df.iterrows():
        row = [None if pd.isna(value) else value for value in row]
        
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?']*len(df.columns))})"
        cursor.execute(insert_query, tuple(row))
        conn.commit()

cursor.close()
conn.close()


