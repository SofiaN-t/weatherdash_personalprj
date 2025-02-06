USE weather_dashboard;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dmi_weather_data';

-- Check null values for suspected columns
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN mean_cloud_cover IS NULL THEN 1 ELSE 0 END) AS null_count,
    (SUM(CASE WHEN mean_cloud_cover IS NULL THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS null_percentage
FROM dmi_weather_data;
-- no non-null values when script mentioned 752 datapoints

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

-- SELECT 
--     CONCAT('SELECT ', 
--         GROUP_CONCAT(CONCAT('\'', COLUMN_NAME, '\' AS column_name, ',
--                            'SUM(', COLUMN_NAME, ' IS NULL) / COUNT(*) AS null_percentage',
--                            ' FROM weather_dashboard HAVING null_percentage > 0.5'
--         ) SEPARATOR ' UNION ALL '),
--     ';') AS query_string
-- FROM INFORMATION_SCHEMA.COLUMNS
-- WHERE TABLE_SCHEMA = 'weather_dashboard' 
-- AND TABLE_NAME = 'dmi_weather_data';


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

SELECT column_name, null_percentage FROM (SELECT 'obs_datetime' AS column_name, SUM(obs_datetime IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_temp' AS column_name, SUM(mean_temp IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'max_temp_w_date' AS column_name, SUM(max_temp_w_date IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'min_temp' AS column_name, SUM(min_temp IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'bright_sunshine' AS column_name, SUM(bright_sunshine IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_radiation' AS column_name, SUM(mean_radiation IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_relative_hum' AS column_name, SUM(mean_relative_hum IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_wind_speed' AS column_name, SUM(mean_wind_speed IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_wind_dir' AS column_name, SUM(mean_wind_dir IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'mean_cloud_cover' AS column_name, SUM(mean_cloud_cover IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data UNION ALL SELECT 'acc_precip' AS column_name, SUM(acc_precip IS NULL) / COUNT(*) AS null_percentage FROM dmi_weather_data) AS subquery WHERE null_percentage > 0.5;


