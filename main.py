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
    file_path = "E:\\Books.csv"
    file_path2="E:\\Ratings.csv"
    file_path3="E:\\Users.csv"
    df = pd.read_csv(file_path,low_memory=False, encoding="utf-8")
    print(df)
    df2=pd.read_csv(file_path2,low_memory=False,encoding="utf-8")
    df3 = pd.read_csv(file_path3, low_memory=False, encoding="utf-8")
    conn_str = "mysql+mysqlconnector://hanoomac_dataeng:LetsD0C0nnect@txpro1.fcomet.com:3306/hanoomac_dataeng"
    print("connection string set")


    engine = create_engine(conn_str, echo=True)

    df.to_sql('BOOKS', engine, if_exists='replace')
    df2.to_sql('Rating',engine,if_exists='replace')
    df3.to_sql('Users', engine, if_exists='replace')
    # read table data using sql query
    sql_df = pd.read_sql(
        "SELECT * FROM BOOKS",
        con=engine
    )
    print("Read from Table BOOKS.")
    print(sql_df)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
