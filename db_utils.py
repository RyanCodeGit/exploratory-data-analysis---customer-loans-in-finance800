import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine

# Creating a function to load credentials from a YAML file
def loader(filename):
    file = open(filename, 'r') # specify file to be loaded
    opened_file = yaml.safe_load(file) # loading the file previously specified
    file.close() # closing file
    return opened_file

credentials = loader('credentials.yaml')

# Creating a class to extract data from RDS database
class RDSDatabaseConnector:
    def __init__(self, creds):
        self.creds = creds

    def start_sqlalchemy_engine(self):
        self.engine = create_engine(f"postgresql+psycopg2://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}")

    def get_data(self, table_name):
        self.df = pd.read_sql_table(f'{table_name}', self.engine)
        return self.df

    def export_to_csv(self, path):
        self.df.to_csv(f'{path}.csv')

    def read_csv(self, path):
        df = pd.read_csv(f'{path}', index_col=0)
        return df

# Testing methods below, will remove later

# test = RDSDatabaseConnector(credentials)
# test.start_sqlalchemy_engine()
# test.get_data('loan_payments')
# test.export_to_csv("loan payments")
# df = test.read_csv("loan payments.csv")
# df.head(10)

""" 
TO DO
1. Add if/else statement to export_to_csv and read_csv methods covering user error
such as not including file extension
"""