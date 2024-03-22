import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import connectors
import sys
import configparser

def get_Connection_string(config_file):
    config = configparser.ConfigParser()

    config.read(config_file)

    return config['conn_str']['connection_string']
def main(file_path, config_file, table_name):
    print(f"datafile_path is ,{file_path}")

    df = pd.read_csv(file_path, low_memory=False, encoding="utf-8")
    print(df)
    print (df.columns)

    conn_str = get_Connection_string(config_file)
    engine = create_engine(conn_str, echo=True)

    # Use prepared statements for SQL UPDATE
    with engine.connect() as conn:
        for index, row in df.iterrows():
            print(row['Publisher'])
            # Example prepared statement for updating a single row
            #update_query = "UPDATE BOOKS SET Publisher = %pub WHERE ISBN in('0195153448','0002005018','0060973129')"
            #conn.execute(update_query, (row['Publisher'])   )
            conn.execute("UPDATE BOOKS SET Publisher = :name WHERE ISBN in('0195153448','0002005018','0060973129')", dict(name='Ask'))

    print("Data Updated in table:", table_name)


if __name__ == "__main__":
    file_path = sys.argv[1]
    config_file = sys.argv[2]
    table_name = sys.argv[3]

    main(file_path, config_file, table_name)