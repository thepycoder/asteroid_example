"""This is a mock module and should be replaced with your actual database connector."""
from pathlib import Path
import pandas as pd
from pandasql import sqldf


def query_database_to_df(query='SELECT * FROM df'):
    # Get the data as CSV
    data_path = Path('data/nasa.csv')
    out_path = Path('/tmp/nasa.csv')

    # Create a dataframe as mock for the database
    df = pd.read_csv(data_path)

    # Add some mock dates
    df['year'] = 2000 * [2021] + (len(df) - 2000) * [2022]

    # Query the df base on the argument
    df = sqldf(query, locals())

    # Save resulting DF to disk so it can be added to a clearml dataset as a file
    df.to_csv(out_path)

    return df, out_path