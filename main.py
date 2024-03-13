# This is a sample Python script.
import pandas as pd
from sqlalchemy import create_engine
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    file_path = "/Users/sgali@us.ibm.com/Downloads/Books1.csv"
    df = pd.read_csv(file_path, encoding="utf-8")
    print(df)
    conn_str = "mysql+mysqlconnector://hanoomac_dataeng:LetsD0C0nnect@txpro1.fcomet.com:3306/hanoomac_dataeng"
    print("connection string set")


    engine = create_engine(conn_str, echo=True)

    df.to_sql('BOOKS', engine, if_exists='replace')

    # read table data using sql query
    sql_df = pd.read_sql(
        "SELECT * FROM BOOKS",
        con=engine
    )

    print(sql_df)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
