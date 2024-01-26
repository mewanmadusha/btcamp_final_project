# 3rd option analyse covid 19 data set with standalone python sql filtering

import snowflake.connector
import pandas as pd

# Step 2: Connect to Snowflake and fetch COVID-19 dataset
conn = snowflake.connector.connect(
    user='mewanmadhusha',
    password='/Applejg8269',
    account='whvtigk-as57496',
    warehouse='COMPUTE_WH',
    database='COVID19_EPIDEMIOLOGICAL_DATA',
    schema='PUBLIC'
)

# we can put any advanced query here
query = 'SELECT * FROM OWID_VACCINATIONS LIMIT 10'

# Fetch data into a Pandas DataFrame for analysis
covid_data = pd.read_sql(query, conn)

# Step 3: Explore the dataset
# Display basic information about the dataset, such as columns, data types, and null values
print("Dataset Info:")
print(covid_data.info())


# Close the Snowflake connection
conn.close()