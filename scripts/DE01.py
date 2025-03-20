import pandas as pd
import pyodbc

# Connection string (update with your details)
conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                      "SERVER=deproject01.database.windows.net;"
                      "DATABASE=deproject01;"
                      "UID=barnaby.rumbold;"
                      "PWD=PWD")

cursor = conn.cursor()

# Path to CSV
path_to_csv = r'C:\Downloads\Barnaby.Rumbold\Downloads\Crime_Data_from_2020_to_Present.csv'

# Read CSV
df = pd.read_csv(path_to_csv,nrows=1)

# Ensure that data is in the right format
df['DR_NO'] = pd.to_numeric(df['DR_NO'], errors='coerce')
df['DR_NO'] = df['DR_NO'].astype(int) # Ensure DR_NO is numeric (INT
df['Date Rptd'] = pd.to_datetime(df['Date Rptd'], errors='coerce').dt.date  # Convert to DATE
df['DATE OCC'] = pd.to_datetime(df['DATE OCC'], errors='coerce').dt.date  # Convert to DATE
df['Crm Cd Desc'] = df['Crm Cd Desc'].astype(str)  # Convert to string (NVARCHAR)
df['Premis Desc'] = df['Premis Desc'].astype(str)  # Convert to string (NVARCHAR)
df['Vict Age'] = pd.to_numeric(df['Vict Age'], errors='coerce')
df['Vict Age'] = df['Vict Age'].astype(float)# Ensure Vict Age is numeric (FLOAT)

cursor.execute("SET IDENTITY_INSERT la_crime ON")

# Insert data row by row
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO la_crime ([DR_NO], [Date Rptd], [DATE OCC], [Crm Cd Desc], [Premis Desc], [Vict Age])
        VALUES (?, ?, ?, ?, ?, ?)
    """, 
    row['DR_NO'], row['Date Rptd'], row['DATE OCC'], row['Crm Cd Desc'], row['Premis Desc'], row['Vict Age'])
    
# Turn off IDENTITY_INSERT after insertion
cursor.execute("SET IDENTITY_INSERT la_crime OFF")

conn.commit()
cursor.close()
conn.close()
