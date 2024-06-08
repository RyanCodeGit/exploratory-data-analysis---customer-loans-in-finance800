import yaml
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