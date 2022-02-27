
def sufficient_data_test(df):
    '''Takes in a df and checks if there are enough sites (>=5) in each location to run a Kruskal Wallis test 
    Returns TRUE/FALSE'''
    import pandas as pd
    import numpy as np

    site_nums = df.groupby('location').agg("nunique")["site_indicator"]
    enough_sites = ~(site_nums[site_nums < 5].any())

    if enough_sites:    
        print ("There are enough sites in each location to run an analysis on this pollutant")
    else:
        print ("There aren't enough sites in each location to run statistical analysis on this pollutant")

    return enough_sites

if __name__ == '__main__':
    # Run on all pollutants ()
    from transform_data import *
    file_path = "./cleaned_data"
    for pol_code in ["NO2", "PM2_5", "PM10"]:
        print("checking data for",pol_code)
        df = transform_data(pol_code, file_path)[4]
        sufficient_data_test(df)