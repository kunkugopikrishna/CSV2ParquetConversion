# Processing Weather data and converting CSV to Parquet

Pre- Requisites:
---------------

1.Python ( version > 3.5).
2.Python packages listed in requirements.txt file. 


Process
--------

1. Verify the Input Directory, presence of files and return corresponding warnings as applicable.
2. Verify the Ouput Directory, create it if not exists.
3. Reading CSV file by using Pandas IO operations.
4. Renaming column names from mixed case letters(sentence case) to lower case with underscore.
5. Converting date field with different format to pandas/SQL date format
6. Replace Null values to zero for float fields
7. Replacing country field with corresponding value if it is NUll by using region field\
8. Adding screen_temperature_day_max field to capture daily max temperature
9. Adding screen_temperature_monthly_rank field to capture monthly rank of a day in terms of the highest temperature


Assumptions
-----------

1. Input and Output directories defined in constants or Environment Variables.
2. Converting single or multiple CSV files into single parquet file.
2. Converted all column names from mixed case letters (sentence case) to lower case with underscore.
3. Considering region field is not null so that can update country field if it is null by using region field.
4. We are generating derived field screen_temperature_day_max to capture max temperature at each day level.
5. We are generating derived field screen_temperature_monthly_rank to capture monthly rank of a day in terms of max temperature.


Output Fields
-------------

forecast_site_code
observation_time
observation_date
wind_direction
wind_speed
wind_gust
visibility
screen_temperature
pressure 
significant_weather_code
site_name
latitude
longitude
region
country
screen_temperature_day_max
screen_temperature_monthly_rank


Unit Test Cases
---------------

Below scenarios have been covered in unit test cases (tests.py).
1. When Input Directory is missing.
2. When Input Directory is Empty.
3. Happy case i.e. when valid input csv files present in input directory.
