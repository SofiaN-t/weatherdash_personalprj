# dmi-station.py --- Get Daisy hourly weather data from a DMI station.
# https://daisy.ku.dk/tools-and-guides/dmi-weather-data/

# Where we want data from
LATITUDE=56.1629 #Aarhus
LONGITUDE=10.2039 

# What data do we want
PARS = ["mean_temp", "mean_daily_max_temp", "mean_daily_min_temp", "max_temp_w_date", "min_temp",
        "no_summer_days", "no_cold_days", "bright_sunshine", "mean_radiation",
        "mean_relative_hum", "mean_wind_speed", "mean_wind_dir", "mean_cloud_cover",
        "acc_precip", "no_days_acc_precip_01", "no_days_acc_precip_1", "no_days_acc_precip_10"]

# Time resolution 
TIMERES = "day"

#Where we want data to go
# OUTPUT_FILE = "dmidata.csv"

# Database Configuration
DB_HOST = "localhost"  
DB_USER = "root"  # Change to your MySQL username
DB_PASSWORD = "Kapodistriako01!"  # Change to your MySQL password
DB_NAME = "weather_dashboard"  # Change to your database name
TABLE_NAME = "dmi_weather_data"  # Change to your table name

#Store information about data here
META_FILE = "dmimeta.csv"

# Your API key
DMI_API_KEY = "5586f407-19de-43de-bf44-0506555ecc03"

print ("Starting script")

from datetime import datetime
import numpy as np
import pandas as pd
from math import cos, asin, sqrt, pi

from sqlalchemy import create_engine
import requests
from tenacity import retry, stop_after_attempt, wait_random

# Function to write DataFrame to MySQL
def write_to_database(df, table_name):
    # Reset index to ensure timestamp is saved as a column
    df = df.reset_index()
    # Rename the column from 'index'
    df.rename(columns={"index": "obs_datetime"}, inplace=True)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    with engine.connect() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Data written to {DB_NAME}.{table_name}")

class DMIOpenDataClient:
    _base_url = "https://dmigw.govcloud.dk/{version}/{api}"

    def __init__(self, api_key: str, api_name:str = "climateData", version: str = "v2"):
        if api_key is None:
            raise ValueError(f"Invalid value for `api_key`: {api_key}")
        if api_name not in ("climateData", "metObs"):
            raise NotImplementedError(f"Following api is not supported yet: {api_name}")
        if version == "v1":
            raise ValueError(f"DMI metObs v1 not longer supported")
        if version not in ["v2"]:
            raise ValueError(f"API version {version} not supported")

        self.api_key = api_key
        self.api_name = api_name
        self.version = version

    def base_url(self, api: str):
        if api not in ("climateData", "metObs"):
            raise NotImplementedError(f"Following api is not supported yet: {api}")
        return self._base_url.format(version=self.version, api=api)

    @retry(stop=stop_after_attempt(10), wait=wait_random(min=0.1, max=1.00))
    def _query(self, api: str, service: str, params, **kwargs):
        res = requests.get(
            url=f"{self.base_url(api=api)}/{service}",
            params={
                "api-key": self.api_key,
                **params,
            },
            **kwargs,
        )
        data = res.json()
        http_status_code = data.get("http_status_code", 200)
        if http_status_code != 200:
            message = data.get("message")
            raise ValueError(
                f"Failed HTTP request with HTTP status code {http_status_code} and message: {message}"
            )
        return res.json()

    def get_stations(
            self, limit = 10000, offset = 0
    ):
        """Get DMI stations.

        Args:
            limit (Optional[int], optional): Specify a maximum number of stations
                you want to be returned. Defaults to 10000.
            offset (Optional[int], optional): Specify the number of stations that should be skipped
                before returning matching objects. Defaults to 0.

        Returns:
            List[Dict[str, Any]]: List of DMI stations.
        """
        res = self._query(
            api=self.api_name,
            service="collections/station/items",
            params={
                "limit": limit,
                "offset": offset,
            },
        )
        return res.get("features", [])

    def get_observations(
        self,
        parameter = None,
        station_id = None,
        from_time = None,
        to_time = None,
        limit = 10000,
        offset = 0,
    ):
        """Get raw DMI observation.

        Args:
            parameter_id (Optional[Parameter], optional): Returns observations for a specific parameter.
                Defaults to None.
            station_id (Optional[int], optional): Search for a specific station using the stationID.
                Defaults to None.
            from_time (Optional[datetime], optional): Returns only objects with a "timeObserved" equal
                to or after a given timestamp. Defaults to None.
            to_time (Optional[datetime], optional): Returns only objects with a "timeObserved" before
                (not including) a given timestamp. Defaults to None.
            limit (Optional[int], optional): Specify a maximum number of observations
                you want to be returned. Defaults to 10000.
            offset (Optional[int], optional): Specify the number of observations that should be skipped
                before returning matching objects. Defaults to 0.

        Returns:
            List[Dict[str, Any]]: List of raw DMI observations.
        """
        res = self._query(
            api="metObs",
            service="collections/observation/items",
            params={
                "parameterId": parameter,
                "stationId": station_id,
                "datetime": _construct_datetime_argument(
                    from_time=from_time, to_time=to_time
                ),
                "limit": limit,
                "offset": offset,
            },
        )
        return res.get("features", [])

    def get_climate_data(
        self,
        parameter = None,
        station_id = None,
        from_time = None,
        to_time = None,
        time_resolution = None,
        limit = 10000,
        offset = 0,
    ):
        """Get raw DMI climate data.

        Args:
            parameter_id (Optional[ClimateDataParameter], optional): Returns observations for a specific parameter.
                Defaults to None.
            station_id (Optional[int], optional): Search for a specific station using the stationID.
                Defaults to None.
            from_time (Optional[datetime], optional): Returns only objects with a "timeObserved" equal
                to or after a given timestamp. Defaults to None.
            to_time (Optional[datetime], optional): Returns only objects with a "timeObserved" before
                (not including) a given timestamp. Defaults to None.
            time_resolution (Optional[str], optional): Filter by time resolution (hour/day/month/year),
                ie. what type of time interval the station value represents
            limit (Optional[int], optional): Specify a maximum number of observations
                you want to be returned. Defaults to 10000.
            offset (Optional[int], optional): Specify the number of observations that should be skipped
                before returning matching objects. Defaults to 0.

        Returns:
            List[Dict[str, Any]]: List of raw DMI observations.
        """
        res = self._query(
            api="climateData",
            service="collections/stationValue/items",
            params={
                "parameterId": parameter,
                "stationId": station_id,
                "datetime": _construct_datetime_argument(
                    from_time=from_time, to_time=to_time
                ),
                "timeResolution": time_resolution,
                "limit": limit,
                "offset": offset,
            },
        )
        return res.get("features", [])

    def get_closest_station(
            self, latitude: float, longitude: float,
            pars = []
    ):
        """Get closest weather station from given coordinates.

        Args:
            latitude (float): Latitude coordinate.
            longitude (float): Longitude coordinate.

        Returns:
            List[Dict[str, Any]]: Closest weather station.
        """
        stations = self.get_stations()
        closest_station, closests_dist = None, 1e10
        want_pars = set (pars)
        for station in stations:
            coordinates = station.get("geometry", {}).get("coordinates")
            if coordinates is None or len(coordinates) < 2:
                continue
            lat, lon = coordinates[1], coordinates[0]
            if lat is None or lon is None:
                continue

            has_pars = set (station['properties']['parameterId'])
            if (not want_pars.issubset (has_pars)):
                continue
            
            # Calculate distance
            dist = distance(
                lat1=latitude,
                lon1=longitude,
                lat2=lat,
                lon2=lon,
            )

            if dist < closests_dist:
                closests_dist, closest_station = dist, station
        return closest_station


def _construct_datetime_argument(
    from_time = None, to_time = None
) -> str:
    if from_time is None and to_time is None:
        return None
    if from_time is not None and to_time is None:
        return f"{from_time.isoformat()}Z"
    if from_time is None and to_time is not None:
        return f"{to_time.isoformat()}Z"
    return f"{from_time.isoformat()}Z/{to_time.isoformat()}Z"

# dmi-open-data: utils.py

# Constants
CONST_EARTH_RADIUS = 6371       # km
CONST_EARTH_DIAMETER = 12742    # km
#EPOCH = datetime.utcfromtimestamp(0)

# From Stackoverflow answer
# https://stackoverflow.com/a/21623206/2538589
def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two geographical points.

    Args:
        lat1 (float): Latitude of point 1.
        lon1 (float): Longitude of point 1.
        lat2 (float): Latitude of point 2.
        lon2 (float): Longitude of point 2.

    Returns:
        float: Haversine distance in km between point 1 and 2.
    """
    p = pi / 180.0
    a = 0.5 - cos((lat2 - lat1) * p) / 2.0 + cos(lat1 * p) * cos(lat2 * p) * (1.0 - cos((lon2 - lon1) * p)) / 2
    return CONST_EARTH_DIAMETER * asin(sqrt(a))  # 2*R*asin...

# Get time series

print ("Opening DMI client")
client = DMIOpenDataClient(api_key=DMI_API_KEY, api_name="climateData")

def getValue (data):
    for i in data:
        yield i['properties']['value']

def getIndex (data):
    for i in data:
        yield np.datetime64 (i['properties']['to'])#

def getSeries (*, par, stationId, timeres):
    print ("Looking up parameter ", par)
    data = client.get_climate_data(par,
                                   station_id=stationId,
                                   from_time=datetime(2014, 12, 31), #not in original
                                   to_time=datetime(2024,12,31),
                                   time_resolution=timeres,
                                   limit=200000)
    if (len (data) > 0):
        print ("Has ", len (data), " datapoints")
    else:
        print ("No data, ignoring")
    return pd.Series (getValue (data),
                      index=getIndex (data))

def get_data (*, latitude, longitude, timeres, pars):
    p = pd.DataFrame ()
    m = pd.DataFrame (columns=["par", "id", "dist", "lat", "lon"])
    for par in pars:
        print ("Looking for station with ", par)
        station = client.get_closest_station (latitude=latitude,
                                              longitude=longitude,
                                              pars=[par])
        if (not station):
            print ("None found");
            continue
        
        coordinates = station.get("geometry", {}).get("coordinates")
        station_lat, station_lon = coordinates[1], coordinates[0]
        stationId = station['properties']['stationId']
        dist = distance (lat1=latitude, lon1=longitude,
                         lat2=station_lat, lon2=station_lon)
        print ("Found", par, "in station", stationId, dist, "km away")
        new_row = pd.DataFrame({'par': par, 'id': stationId, 'dist': dist,
                                'lat': station_lat, 'lon': station_lon}, index=[0])
        m = pd.concat ([new_row, m.loc[:]]).reset_index(drop=True)
        s = getSeries (par=par, stationId=stationId, timeres=timeres)
        if (len (s) > 0): 
            p[par] = s
    return p, m

# Call it
[p, m] = get_data (latitude=LATITUDE, longitude=LONGITUDE, timeres=TIMERES,
                   pars=PARS)

# Sorting
p.sort_index (inplace=True)

# Write weather data
write_to_database(p, TABLE_NAME)
# p.to_csv (OUTPUT_FILE)

# Write meta
m.to_csv (META_FILE)

# dmi-station.py ends here.
