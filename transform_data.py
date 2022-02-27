import os
from pathlib import Path
import pandas as pd
import numpy as np
from clean_cols import clean_cols



def transform_data(code, folder_path):
    '''
    Function to transform hourly pollutant data for each recording site into aggregated dataframes.
    Receives the pollutant code and path to where data is stored and returns:
    1. all_sites_daily: daily average df
    2. clean_monthly: monthly average df
    3. clean_pre_averages: 6-month pre-intervention average df
    4. clean_post_averages: 6 month post-intervention average df
    5. changes: change in pollutant levels (post - pre) df

    '''
    # OS-agnostic path
    main_path = Path(os.getcwd()) / folder_path

    # import files
    inside = pd.read_csv(main_path / f'{code}_data_insideBorder.csv', index_col=["MeasurementGMT"])
    outside = pd.read_csv(main_path / f'{code}_data_outsideBorder.csv', index_col=["MeasurementGMT"])
    border = pd.read_csv(main_path / f'{code}_data_onBorder.csv', index_col=["MeasurementGMT"])

    #clean column names
    inside = clean_cols(inside)
    outside = clean_cols(outside)
    border = clean_cols(border)

    # format indeces of dataframes as datetime data type
    for pol_df in [inside, outside, border]:
        pol_df.index = pd.to_datetime(pol_df.index, format="%Y/%m/%d %H:%M")

    # Aggregate measurements into daily averages
    inside_daily = inside.groupby(by=[inside.index.to_period("D"), "site_indicator", "weekday", "site_type", "location"]).agg("mean").reset_index()
    outside_daily = outside.groupby(by=[outside.index.to_period("D"), "site_indicator", "weekday", "site_type", "location"]).agg("mean").reset_index()
    border_daily = border.groupby(by=[border.index.to_period("D"),"site_indicator", "weekday", "site_type", "location"]).agg("mean").reset_index()

    # organise all locations into a single df
    all_sites_daily = pd.concat([inside_daily, outside_daily, border_daily])
    all_sites_daily.drop(columns =['hour', 'longitude', 'latitude'], inplace=True)
    all_sites_daily.set_index('MeasurementGMT', inplace=True)
    
    # Aggregate into monthly data excluding sites that don't have the minimum data for that month 
    min_days = 20 # Define minimum days to include site in monthly data
    
    all_sites_daily.index = all_sites_daily.index.to_timestamp()
    data_monthly = all_sites_daily.groupby(["site_indicator", "location", 'site_type', all_sites_daily.index.to_period("M")]).agg("mean").reset_index()
    available_days = all_sites_daily.groupby(["site_indicator", all_sites_daily.index.to_period("M")]).agg("count")["concentration"].reset_index()
    clean_monthly = data_monthly.loc[available_days["concentration"] >=  min_days]

    # Organise by period (pre vs post) excluding sites that don't have enough data
    # looking at 6 months before and 6 months after the interventions were implemented
    min_months = 4 # define minimum num of months to include site in pre- and post-intervention average
    pre_start, pre_end = "2017-05", "2017-10" #define pre-intervention dates
    post_start, post_end = "2019-05", "2019-10" #define post-intervention dates

    clean_monthly.set_index('MeasurementGMT', inplace=True)
    clean_monthly.index =  clean_monthly.index.to_timestamp()
    pre_intervention = clean_monthly.loc[(clean_monthly.index >= pre_start) & (clean_monthly.index <= pre_end)]
    post_intervention = clean_monthly.loc[(clean_monthly.index >= post_start) & (clean_monthly.index <= post_end)]
   

    #average by site 
    pre_averages = pre_intervention.groupby(['site_indicator', 'location', 'site_type']).agg('mean')["concentration"].reset_index()
    available_months = pre_intervention.groupby(["site_indicator"]).agg("count")["concentration"].reset_index()
    clean_pre_averages = pre_averages.loc[available_months["concentration"] >=  min_months]
    clean_pre_averages = clean_pre_averages.assign(time  = 'pre')

    post_averages = post_intervention.groupby(['site_indicator', 'location', 'site_type']).agg('mean')["concentration"].reset_index()
    available_months = post_intervention.groupby(["site_indicator"]).agg("count")["concentration"].reset_index()
    clean_post_averages = post_averages.loc[available_months["concentration"] >=  min_months]
    clean_post_averages = clean_post_averages.assign(time  = 'post')

    #Include only sites for which we have data both before and after intervention
    shared = list(set.intersection(set(clean_pre_averages['site_indicator']),set(clean_post_averages['site_indicator'])))
    clean_pre_averages = clean_pre_averages.loc[clean_pre_averages['site_indicator'].isin(shared)].set_index('site_indicator')
    clean_post_averages = clean_post_averages.loc[clean_post_averages['site_indicator'].isin(shared)].set_index('site_indicator')

    changes = clean_pre_averages.join(clean_post_averages,  on = 'site_indicator', rsuffix='_post')
    changes['deltas'] = changes["concentration" + '_post'] - changes["concentration"]
    changes['percent'] = changes['deltas'] / changes['concentration'] * 100
    changes = changes.drop(columns = ['time', 'time_post', 'location_post', 'site_type_post']).reset_index()

    return all_sites_daily, clean_monthly, clean_pre_averages, clean_post_averages, changes
