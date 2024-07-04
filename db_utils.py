from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml


# Creating a function to load credentials from a YAML file
def load_yaml(filename):
    file = open(filename, 'r') # specify file to be loaded
    opened_file = yaml.safe_load(file) # loading the file previously specified
    file.close() # closing file
    return opened_file

# Creating a class to extract data from RDS database
class RDSDatabaseConnector:
    """
    This class is designed to allow a user to connect to an Amazon RDS SQL
    database with login credentials stored in a local YAML file using 
    SQLalchemy. It can then convert data received from this connection into a 
    Pandas dataframe.

    Attributes:
        self.creds (dict) = A dictionary containing login credentials provided
            by the user.
        self.engine (class) = A SQLalchemy engine which will be used to allow 
            interaction with a database.
        self.df (class) = A Pandas dataframe created by reading a SQL database.

    Args:
        creds (dict) = See 'Attributes'.
    """

    def __init__(self, creds):
        """
        This constructor takes a dictionary of credentials as an argument to
        facilitate use of the start_sqlalchemy_engine method. See
        help(start_sqlalchemy_engine) for an explanation of the required keys
        to be included in this dictionary.
        """
        self.creds = creds

    def start_sqlalchemy_engine(self):
        """
        This method creates a SQLalchemy engine using the credentials given to
        the constructor. The intended use is with a PostgreSQL database, using
        psycopg2 as the SQL adapter. 

        In order for the method to work, the following keys are required in the
        dictionary of credentials:
            'RDS_USER'
            'RDS_PASSWORD'
            'RDS_HOST'
            'RDS_PORT'
            'RDS_DATABASE'
        """
        self.engine = create_engine(f"postgresql+psycopg2://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}")

    def get_data(self, table_name):
        """
        This method uses the built-in Pandas method 'pd.read_sql_table'
        to create a Pandas dataframe from the specified 'table_name' argument.

        'table_name' must be a valid table in the SQL database that was
        specified in self.creds.

        Returns:
            self.df = A Pandas dataframe created from the table specified in 'table_name'.
        """
        self.df = pd.read_sql_table(f'{table_name}', self.engine)
        return self.df

    def export_to_csv(self, path):
        """
        This method uses the built-in Pandas method 'df.to_csv' to convert
        a Pandas dataframe to a CSV file and stored locally.
        """
        if ".csv" in path:
            self.df.to_csv(f'{path}')
        else:
            self.df.to_csv(f'{path}.csv')

    def read_csv(self, path):
        """
        This method uses the built-in Pandas method 'pd.read_csv' to convert
        a local CSV file into a Pandas dataframe.

        Returns:
            self.df = A Pandas dataframe created from the CSV file specified
                in 'path'.
        """
        if ".csv" in path:
            self.df = pd.read_csv(f'{path}', index_col=0)
            return self.df
        else:
            self.df = pd.read_csv(f'{path}.csv', index_col=0)
            return self.df

# Testing methods below, will remove later

# credentials = load_yaml('credentials.yaml')
# test = RDSDatabaseConnector(credentials)
# test.start_sqlalchemy_engine()
# df = test.get_data('loan_payments')
# # test.export_to_csv("loan payments")
# # df = test.read_csv("loan payments.csv")
# df.head(10)

