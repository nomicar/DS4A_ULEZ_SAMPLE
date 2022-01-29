import os
from pathlib import Path

import pandas as pd
import numpy as np
import scipy as sci
from scipy import stats
import datetime


def import_data(code, folder_path):
    # OS-agnostic path
    main_path = Path(os.getcwd()) / folder_path

    # import files here
    inside = pd.read_csv(main_path / f'{code}_data_insideBorder.csv', index_col=["MeasurementGMT"])
    outside = pd.read_csv(main_path / f'{code}_data_outsideBorder.csv', index_col=["MeasurementGMT"])
    border = pd.read_csv(main_path / f'{code}_data_onBorder.csv', index_col=["MeasurementGMT"])

    # format indeces of dataframes as datetime data type
    for pol_df in [inside, outside, border]:
        pol_df.index = pd.to_datetime(pol_df.index, format="%Y/%m/%d %H:%M")

    # get the name of the pollutant column in the dataframe from the pollutant code
    pollutant_to_column = {"NO2": "NO2 (ug/m3)",
                           'PM10': 'PM10 (ug/m3)',
                           'PM2_5': 'PM2.5 (ug/m3)', }
    pollutant_column_name = pollutant_to_column[code]

    # inside daily average
    inside_daily = inside.groupby(by=[inside.index.to_period("D"), "Site Indicator", "Weekday", "Site Type", "Location"]).agg(np.mean).reset_index()

    # outside daily average
    outside_daily = outside.groupby(by=[outside.index.to_period("D"), "Site Indicator", "Weekday", "Site Type", "Location"]).agg(np.mean).reset_index()

    # border daily average
    border_daily = border.groupby(by=[border.index.to_period("D"),"Site Indicator", "Weekday", "Site Type", "Location"]).agg(np.mean).reset_index()

    # organise all locations into a single df
    all_sites_daily = pd.concat([inside_daily, outside_daily, border_daily])
    all_sites_daily.drop(columns =['Hour', 'Longitude', 'Latitude'], inplace=True)
    all_sites_daily.set_index('MeasurementGMT', inplace=True)
    
    # Organise into monthly data excluding sites that don't have enough that month 
    min_days = 20 # ~70%
    
    all_sites_daily.index = all_sites_daily.index.to_timestamp()
    data_monthly = all_sites_daily.groupby(["Site Indicator", "Location", 'Site Type', all_sites_daily.index.to_period("M")]).agg("mean").reset_index()
    available_days = all_sites_daily.groupby(["Site Indicator", all_sites_daily.index.to_period("M")]).agg("count")[pollutant_column_name].reset_index()
    clean_monthly = data_monthly.loc[available_days[pollutant_column_name] >=  min_days]

    # Organise by year excluding sites that don't have enough data for that year
    # looking at oct 2016 - sept 2017 specifically
    min_months = 4 #66%

    clean_monthly.set_index('MeasurementGMT', inplace=True)
    clean_monthly.index =  clean_monthly.index.to_timestamp()
    yearly_1617 = clean_monthly.loc[(clean_monthly.index >= "2017-05") & (clean_monthly.index <= "2017-10")]
    yearly_1718 = clean_monthly.loc[(clean_monthly.index >= "2019-05") & (clean_monthly.index <= "2019-10")]
   

    #average by site 
    pre_averages = yearly_1617.groupby(['Site Indicator', 'Location', 'Site Type']).agg('mean')[pollutant_column_name].reset_index()
    available_months = yearly_1617.groupby(["Site Indicator"]).agg("count")[pollutant_column_name].reset_index()
    clean_pre_averages = pre_averages.loc[available_months[pollutant_column_name] >=  min_months]
    clean_pre_averages = clean_pre_averages.assign(Time  = 'pre')

    post_averages = yearly_1718.groupby(['Site Indicator', 'Location', 'Site Type']).agg('mean')[pollutant_column_name].reset_index()
    available_months = yearly_1718.groupby(["Site Indicator"]).agg("count")[pollutant_column_name].reset_index()
    clean_post_averages = post_averages.loc[available_months[pollutant_column_name] >=  min_months]
    clean_post_averages = clean_post_averages.assign(Time  = 'post')

    #Include only sites for which we have yearly data both before and after intervention
    shared = list(set.intersection(set(clean_pre_averages['Site Indicator']),set(clean_post_averages['Site Indicator'])))
    clean_pre_averages = clean_pre_averages.loc[clean_pre_averages['Site Indicator'].isin(shared)].set_index('Site Indicator')
    clean_post_averages = clean_post_averages.loc[clean_post_averages['Site Indicator'].isin(shared)].set_index('Site Indicator')

    changes = clean_pre_averages.join(clean_post_averages,  on = 'Site Indicator', rsuffix='_post')
    changes['deltas'] = changes[pollutant_column_name + '_post'] - changes[pollutant_column_name]
    changes['percent'] = changes['deltas'] / changes[pollutant_column_name] * 100
    changes = changes.drop(columns = ['Time', 'Time_post', 'Location_post', 'Site Type_post']).reset_index()

    return all_sites_daily, clean_monthly, clean_pre_averages, clean_post_averages, changes





if __name__ == '__main__':
    # Enter folder name of main data files here.
    # If files are in the root directory of the repo, leave it an empty string
    file_path = "../cleaned_data"

    pollutant_code = "PM10"  # options: "NO2", "PM2_5", "PM10", "traffic"

    all_sites_daily, clean_monthly, clean_pre_averages, clean_post_averages, deltas = import_data(pollutant_code, file_path)

