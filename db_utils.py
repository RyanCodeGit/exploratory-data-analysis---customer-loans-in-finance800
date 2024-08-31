from sqlalchemy import create_engine
import missingno as msno
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


class DataTransform:
    def __init__(self, df, columns):
        self.df = df
        self.columns = columns
    
    def to_category(self):
        if type(self.columns) == list:
            for column in self.columns:
                self.df[column] = self.df[column].astype('category')
        else:
            self.df[self.columns] = self.df[self.columns].astype('category')
        return self.df

    def to_datetime(self):
        if type(self.columns) == list:
            for column in self.columns:
                self.df[column] = pd.to_datetime(self.df[column], format='mixed')
        else:
            self.df[self.columns] = pd.to_datetime(self.df[self.columns], format='mixed')
        return self.df

    def to_int(self):
        if type(self.columns) == list:
            for column in self.columns:
                self.df[column] = pd.to_numeric(self.df[column], downcast='signed')
        else:
            self.df[self.columns] = pd.to_numeric(self.df[self.columns], downcast='signed')
        return self.df
        
class DataFrameInfo:
    def __init__(self, df):
        self.df = df

    def check_stats(self, column):
        print(f"Median of {column}: {self.df[column].median()}")
        print(f"Mean of {column}: {self.df[column].mean()}")
        print(f"Standard Deviation of {column}: {self.df[column].std()}")
        print(f"Largest value in {column}: {self.df[column].max()}")
        print(f"Smallest value in {column}: {self.df[column].min()}")

    def count_category(self, column):
        print(f"Number of distinct values in {column}: {self.df[column].count()}")
        print(f"Frequency of each unique value in {self.df[column].value_counts(dropna=False)}")

    def count_nulls(self, column):
        print(f"Percentage of non-null data in {column}: {round((self.df[column].count() / self.df.shape[0]) * 100, 3)}")
        print(f"Percentage of null data in {column}: {round(self.df[column].isna().mean() * 100, 3)}")
    
    def print_nulls(self):
        print(f"Percentage of nulls for each column in dataframe")
        print(self.df.isnull().sum()/len(self.df)*100)

    def print_shape(self):
        print(f"Shape of the data in dataframe")
        print(f"Number of rows in dataframe: {self.df.shape[0]}")
        print(f"Number of columns in dataframe: {self.df.shape[1]}")

class Plotter:
    def __init__(self, df):
        self.df = df

    def plot_nulls(self, filter=''):
        if isinstance(filter, pd.core.frame.DataFrame):
            msno.matrix(filter)
        else:
            msno.matrix(self.df)


class DataFrameTransform:
    def __init__(self, df):
        self.df = df

    def impute_median(self, column):
        self.df[column] = self.df[column].fillna(self.df[column].median())

    def impute_mean(self, column):
        self.df[column] = self.df[column].fillna(self.df[column].mean())

    def impute_mode(self, column):
        self.df[column] = self.df[column].fillna(self.df[column].mode())

    def impute_from_col(self, column1, column2):
        """
        This method takes parameters column1 and column2, using values from 
        column2 to fill NULL values in column1.
        """
        self.df[column1] = self.df[column1].fillna(self.df[column2])

""" 
Step 1: You will want to create two classes at this stage:

A Plotter class to visualise insights from the data
A DataFrameTransformclass to perform EDA transformations on your data

Step 2: Use a method/function to determine the amount of NULLs in each column. Determine which columns should be dropped and drop them.

Step 3: Within your DataFrameTransform class create a method which can impute your DataFrame columns. Decide whether the column should be imputed with the median or the mean and impute the NULL values.

Step 4: Run your NULL checking method/function again to check that all NULLs have been removed. Generate a plot by creating a method in your Plotter class to visualise the removal of NULL values.

Step 5: At this point you may want to save a separate copy of your DataFrame that you can use during your analysis in Milestone 4.

"""