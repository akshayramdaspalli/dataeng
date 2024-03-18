from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
import sys
import pandas as pd

# Database Connection
DATABASE_URI = 'mysql+mysqlconnector://hanoomac_dataeng:LetsD0C0nnect@txpro1.fcomet.com:3306/hanoomac_dataeng'
engine = create_engine(DATABASE_URI)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

# Reflect Table Structure
metadata = MetaData()
metadata.reflect(bind=engine)

################## READ DATA ###################
table_name = 'Users'  # Replace with your table name
table = metadata.tables[table_name]

# Query and Print Records
results = session.query(table).all()

for row in results:
    # Adjust according to your table structure
    print(row)
session.close()

################## UPDATE DATA ###################
print("Updating data...")
# Prepare Update Statement
# Update statement
update_query = text("""
    UPDATE Users 
    SET Age = :new_age 
    WHERE UserId = 1
""")

# Parameters
params = {
    "new_age": 11
}

# Execute update
session = Session()
try:
    session.execute(update_query, params)
    session.commit()
    print("Records updated successfully.")
except Exception as e:
    session.rollback()  # Rollback changes on error
    print(f"An error occurred: {e}")
finally:
    session.close()

################## DELETE DATA ###################
print("Deleting data...")
# SQL DELETE statement
delete_query = text("""
    DELETE FROM Users
    WHERE UserID = :user_id
""")

# Condition parameter
params = {"user_id": 8}

# Execute delete operation
session = Session()
try:
    session.execute(delete_query, params)
    session.commit()
    print("Records deleted successfully.")
except Exception as e:
    session.rollback()
    print(f"An error occurred: {e}")
finally:
    session.close()

################## INSERT DATA ###################
print("Inserting data...")
# Load data from CSV
csv_file_path = sys.argv[1]
df = pd.read_csv(csv_file_path)
# Insert data into MySQL table
# df.to_sql(name='Users', con=engine, if_exists='append', index=False)
df.to_sql(name='Users', con=engine, if_exists='replace', index=False)

print("Data inserted successfully.")