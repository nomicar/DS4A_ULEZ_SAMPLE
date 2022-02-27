
import pandas as pd

def clean_cols(df):
    '''Function to organise dataframes columns.'''
    # Change the pollutant column name
    df.columns = ['concentration' if x =='NO2 (ug/m3)' or x=='PM2.5 (ug/m3)' or x=='PM10 (ug/m3)' else x for x in df.columns]
    # change all column names to lowercase
    df = df.rename(columns=str.lower)
    #remove spaces from column names
    df.columns = df.columns.str.replace(' ', '_')

    return df
