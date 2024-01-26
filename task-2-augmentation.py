import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt

# Step 2: Connect to Snowflake and fetch COVID-19 dataset
conn = snowflake.connector.connect(
    user='mewanmadhusha',
    password='/Applejg8269',
    account='whvtigk-as57496',
    warehouse='COMPUTE_WH',
    database='COVID19_EPIDEMIOLOGICAL_DATA',
    schema='PUBLIC'
)

# categorized based on country and while categorize get the sum of Death count
query = 'SELECT COUNTRY_REGION,SUM(DEATHS) AS TotalDeaths FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION'

# Fetch data into a Pandas DataFrame for analysis
covid_data = pd.read_sql(query, conn)

happy_table_2020 = pd.read_csv('2020.csv')

# https://www.kaggle.com/code/agirlcoding/happyness-report/input?select=2020.csv
happy_table_2019 = pd.read_csv('2019.csv')

merged_data = pd.merge(covid_data, happy_table_2019, left_on='COUNTRY_REGION', right_on='Country or region', how='inner')

plt.figure(figsize=(8, 6))

# Scatter plot of Healthy life expectancy vs DEATHS count
plt.scatter(merged_data['Healthy life expectancy'], merged_data['TOTALDEATHS'], alpha=0.5, color='blue')
plt.title('Healthy Life Expectancy 2019(before covid) vs DEATHS Count Covid pandemic')
plt.xlabel('Healthy Life Expectancy 2019')
plt.ylabel('DEATHS Count')

for i, row in merged_data.iterrows():
    plt.text(row['Healthy life expectancy'], row['TOTALDEATHS'], row['COUNTRY_REGION'], fontsize=8)



# next compare with 2020 dataset

merged_data2 = pd.merge(covid_data, happy_table_2020, left_on='COUNTRY_REGION', right_on='Country name', how='inner')

plt.figure(figsize=(8, 6))

# Scatter plot of Healthy life expectancy vs DEATHS count
plt.scatter(merged_data2['Healthy life expectancy']/100, merged_data2['TOTALDEATHS'], alpha=0.5, color='blue')
plt.title('Healthy Life Expectancy 2020(Covid ongoing) vs DEATHS Count')
plt.xlabel('Healthy Life Expectancy 2020')
plt.ylabel('DEATHS Count')

for i, row in merged_data2.iterrows():
    plt.text(row['Healthy life expectancy']/100, row['TOTALDEATHS'], row['COUNTRY_REGION'], fontsize=8)


conn.close()