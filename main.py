import pandas as pd
from sqlalchemy import create_engine
import configparser
import sys


def get_Connection_string(config_file):
    config = configparser.ConfigParser()

    config.read(config_file)

    return config['conn_str']['connection_string']


def main(file_path, config_file, table_name):
    print(f"datafile_path is ,{file_path}")

    df = pd.read_csv(file_path, low_memory=False, encoding="utf-8")
    print(df)
    conn_str = get_Connection_string(config_file)
    engine = create_engine(conn_str, echo=True)

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("table created without Index")
    print("Data Inserted")


if __name__ == "__main__":
    file_path = sys.argv[1]
    config_file = sys.argv[2]
    table_name = sys.argv[3]

main(file_path, config_file, table_name)