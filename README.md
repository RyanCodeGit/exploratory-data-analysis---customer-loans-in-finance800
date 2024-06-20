# Exploratory Data Analysis - Customer Loans in Finance

## Table of Contents
1. [Project Details](#project-details)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [License Information](#license-information)

## Project Details
### Description
I will be practicing conducting exploratory data analysis on a dataset of customer loans.

By conducting this analysis I hope to be able to gain a deeper understanding of the risk and return associated with the business' loans.

### What I've Learned

## Installation Instructions
The following python packages are necessary to use this repository:
1. pandas
2. psycopg2
3. sqlalchemy
4. yaml

## Usage Instructions
The "db_utils.py" file in the home directory is intended to be used to load in login credentials for an RDS database contained in a .yaml file, then connect to this database using the RDSDatabaseConnector class. The user will have to provide their own login credentials in a 'credentials.yaml' file in the home directory to use the function and class methods available in this script.

## File Structure
The home directory of this repository contains most of what is needed, with my personal "credentials.yaml" file omitted for security. 
- "db_utils.py" is a script designed to load credentials from a .yaml file and use the RDSDatabaseConnector class included in the script to convert SQL data from an Amazon RDS database into a pandas dataframe for analysis. It also includes a method to export data from a pandas dataframe to .csv format and to convert .csv formatted data back into a pandas dataframe.
- "loan payments.csv" is a CSV file containing the loan payment dataset that will be analysed in this project.
- "loan_data_dict.md" is included in this repository to familiarise users with the columns present in the database linked to my personal "credentials.yaml" file, which is the same dataset present in the "loan payments.csv" file. I may add this file to the .gitignore at a later date as different databases will likely have different columns.

## License Information
This repository is covered under the MIT License. For more information see the "LICENSE.txt" file.

## To Do List:
