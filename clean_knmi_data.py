# This script cleans the data from KNMI, station de Bilt

# Columns
# DDVEC: wind_dir (in graden (360=noord; 90=oost; 180=zuid; 270=west; 0=windstil/variabel))
# FG: wind_speed (in 0.1 m/s)
# FXX: high_gust (in 0.1 m/s)
# TG: mean_temp (in 0.1 graden Celsius)
# TN: min_temp (in 0.1 graden Celsius)
# TX: max_temp (in 0.1 graden Celsius)
# SQ: sunshine_dur (in 0.1 uur) berekend uit de globale straling (-1 voor <0.05 uur)
# DR: prec_dur (in 0.1 uur)
# RH: prec_am (in 0.1 mm) (-1 voor <0.05 mm)
# UG: rel_hum (in procenten)

# Also check
# https://www.knmi.nl/nederland-nu/klimatologie/daggegevens
# For some descriptions

# Libraries
import csv
import pandas as pd

# Convert txt (downloaded as txt from website) to csv
with open('data\\raw\\result.txt', 'r') as in_file: #https://daggegevens.knmi.nl/klimatologie/daggegevens
    stripped = (line.strip() for line in in_file)
    lines = (line.split(",") for line in stripped if line)
    with open('data\\raw\\dagweerutrecht.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'))
        writer.writerows(lines)

dag_csv = pd.read_csv('data\\clean\\dagweerutrecht.csv')
# Check
dag_csv.loc[:50,]
dag_csv.columns.values
dag_csv.loc[4:50,'B']


## Clean file
# Keep the interesting cols
dag_csv = dag_csv[dag_csv.columns[~dag_csv.columns.isin(['A'])]]
dag_csv.loc[30:40, 'B']

# Remove nas
dag_csv.dropna(axis=0, inplace=True)

# Assign first row as column names
dag_csv.columns = dag_csv.iloc[0]
# Drop first row and reset index
dag_csv = dag_csv[1:].reset_index(drop=True)

# Rename cols
new_col_names = ['Date', 'wind_dir', 'wind_speed', 'high_gust', 'mean_temp', 'min_temp', 'max_temp',
                 'sunshine_dur', 'prec_dur', 'prec_am', 'rel_hum']
rename_dict = dict(zip(dag_csv.columns, new_col_names))
df = dag_csv.rename(columns=rename_dict)

df.head()
df.info()

# Change data types
df = df.astype({
    'wind_dir': 'int',
    'wind_speed': 'float',
    'high_gust': 'float',
    'mean_temp': 'float',
    'min_temp': 'float',
    'max_temp': 'float',
    'sunshine_dur': 'float',
    'prec_dur': 'float',
    'prec_am': 'float',
    'rel_hum': 'int'
})
df['Date'] = pd.to_datetime(df['Date'])
df.loc[df.Date<'2015-01-01']

# Save 
df.to_csv('data/clean/utrecht_weather_timeseries.csv', index=False)

# Check how it looks
# df = pd.read_csv('data/clean/utrecht_weather_timeseries.csv')
# df.head()
# df.mean_temp.mean()

