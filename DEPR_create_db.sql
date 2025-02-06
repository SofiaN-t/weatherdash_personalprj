-- SHOW DATABASES;
DROP TABLE dmi_weather;
CREATE DATABASE IF NOT EXISTS weather_dashboard;
USE weather_dashboard;

CREATE TABLE dmi_weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    obs_datetime DATETIME NOT NULL,
    mean_temp FLOAT,
    mean_max_temp FLOAT,
    mean_min_temp  FLOAT,
    max_temp_w_date FLOAT,
    min_temp FLOAT,
    no_summer_days FLOAT,
    no_cold_days FLOAT,
    bright_sunshine FLOAT,
    mean_radiation FLOAT,
    mean_rel_hum FLOAT,
    mean_wind_speed FLOAT,
    mean_wind_dir FLOAT,
    acc_precip FLOAT,
    no_days_acc_prec_01 FLOAT,
    no_days_acc_prec_1 FLOAT,
    no_days_acc_prec10 FLOAT
);
