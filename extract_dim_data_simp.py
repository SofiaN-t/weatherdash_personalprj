import requests
import mysql.connector
from mysql.connector import Error
from geopy.distance import geodesic

# MySQL Connection Details
MYSQL_HOST = "localhost"
MYSQL_USER = "host"
MYSQL_PASSWORD = "Kapodistriako01!"
MYSQL_DATABASE = "weather_dashboard"

# DMI API Base URLs
STATIONS_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/station/items"
DATA_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"

# DMI API Key (Replace with your API key)
API_KEY = "5586f407-19de-43de-bf44-0506555ecc03"

# Hard-coded parameters
COORDINATES = (56.1629, 10.2039)  # Example: Aarhus (latitude, longitude)
START_DATE = "2023-01-01"
END_DATE = "2023-12-31"
TIME_RESOLUTION = "day"  # Ensures daily values
LIMIT = 100  # Fetch up to x records per request
WEATHER_VARIABLES = ["mean_temp", "acc_precip", "mean_relative_hum"]  # Selected weather variables

# Function to Find the Closest DMI Weather Station
def find_closest_station(lat, lon):
    params = {"api-key": API_KEY}
    response = requests.get(STATIONS_URL, params=params)
    response.raise_for_status()
    stations = response.json()["features"]

    closest_station = None
    min_distance = float("inf")

    for station in stations:
        station_coords = (station["geometry"]["coordinates"][1], station["geometry"]["coordinates"][0])  # (lat, lon)
        distance = geodesic((lat, lon), station_coords).km

        if distance < min_distance:
            min_distance = distance
            closest_station = station["properties"]["stationId"]

    return closest_station

# Function to Fetch Weather Data
def fetch_weather_data(station_id):
    params = {
        "api-key": API_KEY,
        "stationId": station_id,
        "datetime": f"{START_DATE}/{END_DATE}",
        "timeResolution": TIME_RESOLUTION,
        "limit": LIMIT
    }

    response = requests.get(DATA_URL, params=params)
    response.raise_for_status()
    data = response.json()

    filtered_data = []
    for entry in data["features"]:
        properties = entry["properties"]
        date = properties["observationTime"][:10]  # Extract YYYY-MM-DD

        weather_entry = {"date": date}
        for var in WEATHER_VARIABLES:
            weather_entry[var] = properties.get(var, None)  # Get only selected variables

        filtered_data.append(weather_entry)

    return filtered_data

# Function to Insert Data into MySQL
def insert_into_mysql(weather_data):
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()

        # Generate dynamic column names
        columns = ", ".join(["date"] + WEATHER_VARIABLES)
        placeholders = ", ".join(["%s"] * (len(WEATHER_VARIABLES) + 1))
        insert_query = f"INSERT INTO weather_data (city, {columns}) VALUES (%s, {placeholders})"

        for entry in weather_data:
            values = ["Aarhus"] + [entry.get(var, None) for var in WEATHER_VARIABLES]
            cursor.execute(insert_query, values)

        conn.commit()
        print(f"Inserted {cursor.rowcount} rows into MySQL for Aarhus.")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Main Execution
def main():
    station_id = find_closest_station(*COORDINATES)
    if station_id:
        print(f"Closest station ID: {station_id}")

        weather_data = fetch_weather_data(station_id)
        if weather_data:
            insert_into_mysql(weather_data)
        else:
            print("No weather data retrieved.")
    else:
        print("No weather station found.")

if __name__ == "__main__":
    main()
