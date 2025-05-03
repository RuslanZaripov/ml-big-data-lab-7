import os
import argparse
import textwrap
import pandas as pd
from logger import Logger
from clickhouse_driver import Client
from dotenv import load_dotenv


class CassandraDatabaseCreator:
    def __init__(self):
        self.logger = Logger(show=True)
        self.log = self.logger.get_logger(__name__)
        
        self.args = self._parse_arguments()
        
        load_dotenv()
        
        self.PORT = "9000"
        self.IP_ADDRESS = os.environ.get("CLICKHOUSE_IP_ADDRESS", "localhost")
        self.USER = os.environ.get("CLICKHOUSE_USER", "default")
        self.PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.timeout = 60
        
        self.client = None

    def _parse_arguments(self):
        parser = argparse.ArgumentParser(description="Create Database")
        parser.add_argument(
            "--table-name", 
            required=True, 
            help="Table name")
        parser.add_argument(
            "--csv-path", 
            required=True, 
            help="Path to the CSV file")
        parser.add_argument(
            "--delimiter", 
            default=None, 
            help="Delimiter used in the CSV file (default: autodetect)")
        return parser.parse_args()

    def _get_clickhouse_client(self):
        self.log.info(f"Connecting to ClickHouse at {self.IP_ADDRESS}:{self.PORT}")
        client = Client(
            host=self.IP_ADDRESS,
            port=int(self.PORT),
            user=self.USER,
            password=self.PASSWORD,
            connect_timeout=self.timeout
        )
        return client

    def _infer_column_types(self, sample_data):
        column_types = []
        for col, dtype in sample_data.dtypes.items():
            ch_type = 'String'
            col_name = col.replace('-', '_')
            if col == 'code' or col == 'IndexColumn':
                column_types.append(f"{col_name} {ch_type}")
            else:
                column_types.append(f"{col_name} Nullable({ch_type})")
        return column_types
    
    def _create_table(self):
        sample_data = pd.read_csv(
            self.args.csv_path, 
            delimiter=self.args.delimiter, 
            nrows=2,
            engine='python'
        )
        
        column_definitions = self._infer_column_types(sample_data)
        
        self.log.info(f"Dropping table {self.args.table_name} if it exists...")
        self.client.execute(f"DROP TABLE IF EXISTS {self.args.table_name}")
            
        self.log.info(f"Creating table {self.args.table_name}...")
        definitions_str = '\t\t' + ',\n\t\t'.join(column_definitions)
        
        table_creation_command = f"""
        CREATE TABLE {self.args.table_name} (
        {definitions_str}
        )
        ENGINE = MergeTree()
        PRIMARY KEY (code)
        """
        
        self.log.debug(f"Creating table with command:\n{table_creation_command}")
        self.client.execute(table_creation_command)


    def run(self):
        try:
            self.client = self._get_clickhouse_client()
            self._create_table()
            self.log.info("Database and table created successfully!")
            
        except Exception as e:
            self.log.error(f"Error occurred while creating database: {e}")

        finally:
            if self.client is not None:
                self.client.disconnect()


if __name__ == "__main__":
    creator = CassandraDatabaseCreator()
    creator.run()
