from scipy import stats
from statsmodels.graphics.gofplots import qqplot
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import missingno as msno
import numpy as np
import pandas as pd
import plotly.express as px
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
    def to_category(self, df, columns):
        if type(columns) == list:
            for column in columns:
                df[column] = df[column].astype('category')
        else:
            df[columns] = df[columns].astype('category')

    def to_datetime(self, df, columns):
        if type(columns) == list:
            for column in columns:
                df[column] = pd.to_datetime(df[column], format='mixed')
        else:
            df[columns] = pd.to_datetime(df[columns], format='mixed')

    def to_int(self, df, columns):
        if type(columns) == list:
            for column in columns:
                df[column] = pd.to_numeric(df[column], downcast='signed')
        else:
            df[columns] = pd.to_numeric(df[columns], downcast='signed')
        
class DataFrameInfo:
    def calc_data_loss(self, series1, series2):
        """"
        This method takes parameters series1 and series2, 
        counts their values and prints the difference as a percentage.

        Args:
           series1 (Pandas series): The series with the highest count.
           series2 (Pandas series): The series with the lowest count.
        """
        difference = series1.count() - series2.count()
        print(f"Amount of data lost: {((difference / series1.count()) * 100).round(2)}%")
    
    def check_stats(self, df, column):
        print(f"Q1 of {column}: {df[column].quantile(0.25)}")
        print(f"Median of {column}: {df[column].median()}")
        print(f"Q3 of {column}: {df[column].quantile(0.75)}")
        print(f"Mean of {column}: {df[column].mean()}")
        print(f"Mode of {column}: {df[column].mode().iloc[0]}")
        print(f"Standard Deviation of {column}: {df[column].std()}")
        print(f"Smallest value in {column}: {df[column].min()}")
        print(f"Largest value in {column}: {df[column].max()}")

    def contains_zero(self, df, column):
        return (df[column] == 0).any()
        
    def value_count(self, df, column):
        print(f"Number of distinct values in {column}: {df[column].count()}")
        print(f"Frequency of each unique value in {df[column].value_counts(dropna=False)}")

    def count_nulls(self, df, column):
        print(f"Percentage of non-null data in {column}: {round((df[column].count() / df.shape[0]) * 100, 3)}")
        print(f"Percentage of null data in {column}: {round(df[column].isna().mean() * 100, 3)}")
    
    def print_nulls(self, df):
        print(f"Percentage of nulls for each column in dataframe")
        print(df.isnull().sum()/len(df)*100)

    def print_shape(self, df):
        print(f"Shape of the data in dataframe")
        print(f"Number of rows in dataframe: {df.shape[0]}")
        print(f"Number of columns in dataframe: {df.shape[1]}")

class Plotter:
    def null_matrix(self, df):
        msno.matrix(df)

    def multi_hist(self, df, columns=None, num_bins=50, size_inches=(15,20)):
        df.hist(column=columns, bins=num_bins, figsize=size_inches)

    def plot_box(self, df, column, points='outliers'):
        box = px.box(df, y=column, points=points)
        box.show()
    
    def plot_hist(self, df, column=None, bins=None):
        if type(df) == pd.core.frame.DataFrame:
            hist = px.histogram(df, x=column, nbins=bins)
            hist.show()
        elif type(df) == pd.core.series.Series:
            hist = px.histogram(df, x=df, nbins=bins)
            hist.show()

    def plot_qq(self, df, column=None):
        if type(df) == pd.core.frame.DataFrame:
            plot = qqplot(df[column], line='q', fit=True)
            plt.show()
        elif type(df) == pd.core.series.Series:
            plot = qqplot(df, line='q', fit=True)
            plt.show()


class DataFrameTransform:
    def boxcox_tf(self, df, column):
        transform = stats.boxcox(df[column])
        series = pd.Series(transform[0])
        return series
    
    def impute_median(self, df, column):
        df[column] = df[column].fillna(df[column].median())

    def impute_mean(self, df, column):
        df[column] = df[column].fillna(df[column].mean())

    def impute_mode(self, df, column):
        df[column] = df[column].fillna(df[column].mode().iloc[0])

    def impute_from_col(self, df, column1, column2):
        """
        This method takes parameters column1 and column2, using values from 
        column2 to fill NULL values in column1.
        """
        df[column1] = df[column1].fillna(df[column2])

    def log_tf(self, df, column):
        return np.log(df[column])

    def remove_outliers(self, df, column, threshold=3):
        zscore = np.abs(stats.zscore(df[column]))
        outliers = df[zscore > threshold]
        df[column] = df[column].drop(outliers.index)

    def yeojohn_tf(self, df, column):
        transform = stats.yeojohnson(df[column])
        series = pd.Series(transform[0])
        return series

""" 
Removing outliers from the dataset will improve the quality and accuracy of the analysis as outliers can distort the analysis results. You will need to first identify the outliers and then use a method to remove them.

Step 1: First visualise your data using your Plotter class to determine if the columns contain outliers.

Step 2: Once identified use a method to transform or remove the outliers from the dataset. Build this method in your DataFrameTransform class.

Step 3: With the outliers transformed/removed re-visualise your data with you Plotter class to check that the outliers have been correctly removed.

"""