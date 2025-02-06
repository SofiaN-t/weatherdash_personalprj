USE weather_dashboard;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dmi_weather_data';

SELECT * FROM dmi_weather_data LIMIT 10;


-- CLEAN
-- Check null values for suspected columns
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN mean_cloud_cover IS NULL THEN 1 ELSE 0 END) AS null_count,
    (SUM(CASE WHEN mean_cloud_cover IS NULL THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS null_percentage
FROM dmi_weather_data;
-- all non-null values when script mentioned 752 datapoints
SELECT COUNT(DISTINCT mean_cloud_cover) FROM dmi_weather_data;

-- SELECT mean_cloud_cover from dmi_weather_data WHERE mean_cloud_cover IS NOT NULL;

SELECT obs_datetime, mean_cloud_cover
INTO OUTFILE 'C:\\Users\\sophi\\Documents\\PersonalProjects\\code\\weather_dashboard\\data\\raw\\mean_cloud_cover.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM dmi_weather_data;
-- No data, it looks like, no idea what the 752 parameters are

SHOW VARIABLES LIKE 'secure_file_priv';
-- Had to grant permission

-- Check nulls for all columns
SET SESSION group_concat_max_len = 1000000;

-- Construct the query to run
SELECT 
    CONCAT('SELECT column_name, null_percentage FROM (', 
        GROUP_CONCAT(
            CONCAT(
                'SELECT ''', COLUMN_NAME, ''' AS column_name, ',
                'SUM(', COLUMN_NAME, ' IS NULL) / COUNT(*) AS null_percentage ',
                'FROM dmi_weather_data'
            ) 
            SEPARATOR ' UNION ALL '
        ),
    ') AS subquery WHERE null_percentage > 0.5;') 
AS query_string
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'weather_dashboard' 
AND TABLE_NAME = 'dmi_weather_data';

-- Run the query
SELECT column_name, null_percentage FROM (SELECT 'obs_datetime' AS column_name, SUM(obs_datetime IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_temp' AS column_name, SUM(mean_temp IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'max_temp_w_date' AS column_name, SUM(max_temp_w_date IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'min_temp' AS column_name, SUM(min_temp IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'bright_sunshine' AS column_name, SUM(bright_sunshine IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_radiation' AS column_name, SUM(mean_radiation IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_relative_hum' AS column_name, SUM(mean_relative_hum IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_wind_speed' AS column_name, SUM(mean_wind_speed IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_wind_dir' AS column_name, SUM(mean_wind_dir IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_cloud_cover' AS column_name, SUM(mean_cloud_cover IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'acc_precip' AS column_name, SUM(acc_precip IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data) AS subquery WHERE null_percentage > 0.5;
-- only cloud cover with 100% nulls

-- Drop the column
ALTER TABLE dmi_weather_data DROP COLUMN mean_cloud_cover;

-- time is meaningless, remove
ALTER TABLE dmi_weather_data
MODIFY obs_datetime DATE;
ALTER TABLE dmi_weather_data
RENAME COLUMN obs_datetime TO obs_date;


-- EXPLORE
-- Find the windiest days
SELECT obs_date, mean_wind_speed FROM dmi_weather_data ORDER BY mean_wind_speed DESC LIMIT 10;

-- Find the rainiest days
SELECT obs_date, acc_precip FROM dmi_weather_data ORDER BY acc_precip DESC LIMIT 10;

-- Get average temperature by month
SELECT MONTH(obs_date) AS month, AVG(mean_temp) AS avg_temp
FROM dmi_weather_data
GROUP BY month
ORDER BY month;

-- Get average precipitation by month
SELECT MONTH(obs_date) AS month, AVG(acc_precip) AS avg_prec
FROM dmi_weather_data
GROUP BY month
ORDER BY month;

