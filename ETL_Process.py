#!/usr/bin/env python
# coding: utf-8

# # Automated COVID-19 Monitoring ETL Project
# **Author**: Moch Nabil Farras Dhiya
# 
# **E-mail**: nabilfarras923@gmail.com
# 
# **Institution**: Bandung Institute of Technology
# 
# **Student ID**: 10120034
# 
# 
# ---
# 
# **About**: This is a side-project created in order to monitor COVID-19 data from WHO data source by the following steps:
# 
# 
# 1.   Extracting data from WHO Website
# 2.   Transform the data (making sure it is usable and consistent)
# 3.   Upload the final data to Local MySQL Database
# 4.   Visualize the data for end-users by Tableau
# 
# These steps will be automatically implemented on daily basis, so we only need to monitor the dashboard.
# 
# **NOTE**: In this project, I use on-site database in order to minimize the cost as Cloud Databases are mostly paid.

# # Import Modules

# In[1]:


# !pip install requests lxml html5lib beautifulsoup4 sqlalchemy cryptography pymysql pycountry


# In[2]:


# Connect to local
import os

# Extracting file from URL (WHO Data) and scraping data from another websource (Worldometers Data)
import requests
# from bs4 import BeautifulSoup

# Importing and transforming file
import pandas as pd

# Data manipulation
import numpy as np
import re # Cleaning texts
import datetime as dt # Datetime manipulation
import pycountry # List of countries

# Connect Colab with Local MySQL Database and delete existing table
import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text as sa_text

# Notify is there is an error to email
import logging
import logging.handlers

# Import ETL Functions
from ETL_Functions import remove_symbols, get_alphabet, date_parsing, \
                          swap_text, levenshteinDistanceDP, most_similar, \
                          drop_none_other, get_code_from_country, get_key_from_value, get_country_from_code


# # Connect to MySQL Local Database

# In[3]:


print("================================")
print("Connecting to Local DB .....")

# Credentials to database connection
hostname = "localhost"
dbname = "covid_19"
uname = os.getenv('DB_UNAME')
pwd = os.getenv('DB_PASSWORD')

# Create SQLAlchemy engine to connect to MySQL Database
sqlEngine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}" \
				.format(host = hostname,
                db = dbname,
                user = uname,
                pw = pwd))

dbConnection    = sqlEngine.connect()

print("Successfully to Local DB.")
print("================================")
print()


# # Extract WHO File (CSV)

# ## Daily Case and Death

# In[4]:


save_path = 'D:\Kuliah\Project\Python\Automated Covid-19 Monitoring'


# In[5]:


# Get content from URL
daily_case_death_url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
req = requests.get(daily_case_death_url)
url_content = req.content

file_name = 'daily_case_death.csv'
completeName = os.path.join(save_path, file_name)
csv_file_0 = open(completeName, 'wb') # Save to directed folder

csv_file_0.write(url_content)
csv_file_0.close()


# ## Latest Case and Death

# In[6]:


# Get content from URL
latest_case_death_url = 'https://covid19.who.int/WHO-COVID-19-global-table-data.csv'
req = requests.get(latest_case_death_url)
url_content = req.content

file_name = 'latest_case_death.csv'
completeName = os.path.join(save_path, file_name)
csv_file_1 = open(completeName, 'wb') # Save to directed folder

csv_file_1.write(url_content)
csv_file_1.close()


# ## Vaccination Data

# In[7]:


# Get content from URL
vaccine_url = 'https://covid19.who.int/who-data/vaccination-data.csv'
req = requests.get(vaccine_url)
url_content = req.content

file_name = 'vaccination.csv'
completeName = os.path.join(save_path, file_name)
csv_file_2 = open(completeName, 'wb') # Save to directed folder

csv_file_2.write(url_content)
csv_file_2.close()


# ## Vaccine Metadata

# In[8]:


# Get content from URL
vaccine_md_url = 'https://covid19.who.int/who-data/vaccination-metadata.csv'
req = requests.get(vaccine_md_url)
url_content = req.content

file_name = 'vaccination_md.csv'
completeName = os.path.join(save_path, file_name)
csv_file_3 = open(completeName, 'wb') # Save to directed folder

csv_file_3.write(url_content)
csv_file_3.close()


# # Scrape External Data

# In[9]:


# url_main = "https://www.worldometers.info/coronavirus/#main_table"
# url_weekly = "https://www.worldometers.info/coronavirus/weekly-trends/#weekly_table"

# r_main = requests.get(url_main)
# df_main_list = pd.read_html(r_main.text) # this parses all the tables in webpages to a list
# df_main = df_main_list[0]

# r_weekly = requests.get(url_weekly)
# df_weekly_list = pd.read_html(r_weekly.text) # this parses all the tables in webpages to a list
# df_weekly = df_weekly_list[0]


# # Import Data

# In[10]:


print("================================")
print("Importing Data .....")
print()

# Today's Daily Case and Death Data
now_df_daily_cd = pd.read_csv('daily_case_death.csv', index_col = False)

# Today's Latest Case and Death Data
now_df_latest_cd = pd.read_csv('latest_case_death.csv', index_col = False)

# Today's Vaccination Data
now_df_vaccination = pd.read_csv('vaccination.csv', index_col = False) 

# Today's Vaccination Metadata Data
now_df_vaccination_md = pd.read_csv('vaccination_md.csv', index_col = False) 

print("Data have been imported")
print()


# In[11]:


print("Searching for another Data in the Local DB .....")
cond = False # Assuming there is no data in database yet
try:    
    # Yesterday's Daily Case and Death Data
    yesterday_df_daily_cd = pd.read_sql('SELECT * FROM covid_19.daily_case_death', dbConnection);

    # Yesterday's Latest Case and Death Data
    yesterday_df_latest_cd = pd.read_sql('SELECT * FROM covid_19.latest_case_death', dbConnection);

    # Yesterday's Vaccination Data
    yesterday_df_vaccination = pd.read_sql('SELECT * FROM covid_19.daily_vaccination', dbConnection);

    # Yesterday's Vaccination Metadata Data
    yesterday_df_vaccination_md = pd.read_sql('SELECT * FROM covid_19.vaccination_md', dbConnection);
    
    cond = True
    print("Data found.")

except Exception as e:
    print(e)
    print("No Data in Local DB yet.")

print("================================")
print()


# # Initial EDA

# ## Daily Case and Death

# In[12]:


now_df_daily_cd


# In[13]:


now_df_daily_cd['Country'].unique()


# In[14]:


now_df_daily_cd.info()


# We have to change the Date_reported dtypes to date.

# ## Latest Case and Death

# In[15]:


now_df_latest_cd


# In[16]:


now_df_latest_cd = now_df_latest_cd.rename(columns = {'Name': 'Country'})


# In[17]:


now_df_latest_cd['Country'].unique()


# In[18]:


now_df_latest_cd.info()


# We will drop the 'Global' value as it is not neccessary.

# In[19]:


now_df_latest_cd = now_df_latest_cd[now_df_latest_cd['Country'] != 'Global']


# ## Vaccination

# In[20]:


now_df_vaccination


# In[21]:


now_df_vaccination.info()


# We have to change the DATE_UPDATED and FIRST_VACCINE_DATE dtypes to date.

# ## Vaccination Metadata

# In[22]:


now_df_vaccination_md


# In[23]:


now_df_vaccination_md.info()


# :We have to change the START_DATE and END_DATE dtypes to date.

# # Data Cleaning

# In[24]:


print("================================")
print("Cleaning the Data .....")
print()


# ## Set up columns name

# We want our format name to be readable in SQL format as we will import the dataframe to our local DB.

# ## Check Values

# Though it is unlikely, we will make sure that there will not be any negative value(s) in the dataframe as well as numbers in the country names.

# In[25]:


# Check negative values
for df in [now_df_daily_cd, now_df_latest_cd, now_df_vaccination, now_df_vaccination_md]:
    for col in df.columns:
        if (df[col].dtypes == 'int64') or (df[col].dtypes == 'float64'):
            df[col] = df[col].apply(abs)


# In[26]:


for df in [now_df_daily_cd, now_df_latest_cd, now_df_vaccination, now_df_vaccination_md]:
    for col in ['country', 'country_code', 'iso3']:
        try:
            df[col] = df[col].apply(get_alphabet)
            
        except Exception as e:
            pass


# At this point, we can be sure that there will be no numbers or symbols in our country, country_code, and iso3 columns.

# ## Date parsing

# Make sure that all the data format are homogenous.

# In[27]:


# Daily Case and Death
now_df_daily_cd.columns = now_df_daily_cd.columns.to_series().apply(remove_symbols)
for col in now_df_daily_cd.columns:
    if now_df_daily_cd[col].dtype == 'object':
        now_df_daily_cd[col] = now_df_daily_cd[col].apply(date_parsing)

# Latest Case and Death
now_df_latest_cd.columns = now_df_latest_cd.columns.to_series().apply(remove_symbols)
for col in now_df_latest_cd.columns:
    if now_df_latest_cd[col].dtype == 'object':
        now_df_latest_cd[col] = now_df_latest_cd[col].apply(date_parsing)

# Vaccination
now_df_vaccination.columns = now_df_vaccination.columns.to_series().apply(remove_symbols)
for col in now_df_vaccination.columns:
    if now_df_vaccination[col].dtype == 'object':
        now_df_vaccination[col] = now_df_vaccination[col].apply(date_parsing)

# Vaccination Metadata
now_df_vaccination_md.columns = now_df_vaccination_md.columns.to_series().apply(remove_symbols)
for col in now_df_vaccination_md.columns:
    if now_df_vaccination_md[col].dtype == 'object':
        now_df_vaccination_md[col] = now_df_vaccination_md[col].apply(date_parsing)


# ## Datatypes

# In[28]:


# Daily Case and Death
for col in now_df_daily_cd.columns:
    print(f"\n======================= {col} =======================")
    display(now_df_daily_cd[col].value_counts())


# In[29]:


# Latest Case and Death
for col in now_df_latest_cd.columns:
    print(f"\n======================= {col} =======================")
    display(now_df_latest_cd[col].value_counts())


# In[30]:


# Vaccination
for col in now_df_vaccination.columns:
    print(f"\n======================= {col} =======================")
    display(now_df_vaccination[col].value_counts())


# In[31]:


# Vaccination Metadata
for col in now_df_vaccination_md.columns:
    print(f"\n======================= {col} =======================")
    display(now_df_vaccination_md[col].value_counts())


# In[32]:


# Daily Case and Death Data
now_df_daily_cd['date_reported'] = pd.to_datetime(now_df_daily_cd['date_reported'])

# Latest Case and Death Data
# -

# Vaccination Data
now_df_vaccination['date_updated'] = pd.to_datetime(now_df_vaccination['date_updated'])
now_df_vaccination['first_vaccine_date'] = pd.to_datetime(now_df_vaccination['first_vaccine_date'])

# Vaccination Metadata
now_df_vaccination_md['start_date'] = pd.to_datetime(now_df_vaccination_md['start_date'])
now_df_vaccination_md['end_date'] = pd.to_datetime(now_df_vaccination_md['end_date'])


# In[33]:


print("Successfully cleaned the Data.")
print("================================")
print()


# # Handle Missing Value(s)

# Notice that as null values are most probably indicating that there is no report for the given column/criteria at the specific period, then the most logical thing to do leave these null values as it is.

# In[34]:


# -


# # Handle Duplicate Value(s)

# ## WHO Data

# In[35]:


# Daily Case and Death Data
now_df_daily_cd = now_df_daily_cd.drop_duplicates()

# Latest Case and Death Data
now_df_latest_cd = now_df_latest_cd.drop_duplicates()

# Vaccination Data
now_df_vaccination = now_df_vaccination.drop_duplicates()

# Vaccination Metadata
now_df_vaccination_md = now_df_vaccination_md.drop_duplicates()


# # Data Manipulation

# To make our format consistent, we will do the followings::
# 1.  Round up some float numbers to only 3 decimal numbers. We do this because want our data to be as readable as we can. Thus, we need to do some manipulation to the data.
# 2.  Add countrycode feature if it does not exists and convert ISO3 into country code feature, then re-order the columns.
# 3.  Match each country's name in the database with the names written in the dataframe to avoid any ambiguation.
# 4.  Add feature(s) to make sure that the data is not ambiguous.

# In[36]:


print("================================")
print("Manipulating the Data .....")
print()


# ## Round Decimals

# In[37]:


print("Rounding Decimals .....")
print()


# In[38]:


# Daily Case and Death
for col in now_df_daily_cd.columns:
    if now_df_daily_cd[col].dtypes == 'float64':
        now_df_daily_cd[col] = now_df_daily_cd[col].apply(lambda x: round(x, 3))
    
# Latest Case and Death Data
for col in now_df_latest_cd.columns:
    if now_df_latest_cd[col].dtypes == 'float64':
        now_df_latest_cd[col] = now_df_latest_cd[col].apply(lambda x: round(x, 3))
    
# Vaccination Data
for col in now_df_vaccination.columns:
    if now_df_vaccination[col].dtypes == 'float64':
        now_df_vaccination[col] = now_df_vaccination[col].apply(lambda x: round(x, 3))
    
# Vaccination Metadata
for col in now_df_vaccination_md.columns:
    if now_df_vaccination_md[col].dtypes == 'float64':
        now_df_vaccination_md[col] = now_df_vaccination_md[col].apply(lambda x: round(x, 3))


# ## Match Country Name(s) - 1st Layer

# In[39]:


print("Matching Country Name(s) - 1st Layer .....")
print()


# In[40]:


# Get country name from ISO3
country_iso3_mapping = {country.alpha_3: country.name for country in pycountry.countries}

# Get country code from country name
country_code_mapping = {country.name: country.alpha_2 for country in pycountry.countries}


# ### Daily Case and Death

# In[41]:


now_df_daily_cd.columns


# In[42]:


now_df_daily_cd[now_df_daily_cd[['country_code', 'country', 'who_region']]
                .isna().any(axis = 1)].country


# In[43]:


# Store the countries with NaN value
nan_now_df_daily_cd = now_df_daily_cd[now_df_daily_cd[['country_code', 'country', 'who_region']]
                                      .isna().any(axis = 1)].country


# ### Latest Case and Death

# In[44]:


now_df_latest_cd.columns


# In[45]:


now_df_latest_cd['country_code'] = now_df_latest_cd['country'].apply(lambda x: country_code_mapping.get(x))
now_df_latest_cd = now_df_latest_cd[['country_code', 'country', 'who_region', 
                                     'cases_cumulative_total', 'cases_cumulative_total_per_100000_population',
                                     'cases_newly_reported_in_last_7_days',
                                     'cases_newly_reported_in_last_7_days_per_100000_population',
                                     'cases_newly_reported_in_last_24_hours', 'deaths_cumulative_total',
                                     'deaths_cumulative_total_per_100000_population',
                                     'deaths_newly_reported_in_last_7_days',
                                     'deaths_newly_reported_in_last_7_days_per_100000_population',
                                     'deaths_newly_reported_in_last_24_hours']]

now_df_latest_cd[now_df_latest_cd[['country_code', 'country', 'who_region']]
                 .isna().any(axis = 1)]


# In[46]:


# Store the countries with NaN value
nan_now_df_latest_cd = now_df_latest_cd[now_df_latest_cd[['country_code', 'country', 'who_region']]
                                        .isna().any(axis = 1)].country


# ### Vaccination

# In[47]:


now_df_vaccination.columns


# In[48]:


now_df_vaccination['country'] = now_df_vaccination['iso3'].apply(lambda x: country_iso3_mapping.get(x))
now_df_vaccination['country_code'] = now_df_vaccination['country'].apply(lambda x: country_code_mapping.get(x))
now_df_vaccination = now_df_vaccination[['country_code', 'country', 'who_region', 'data_source', 
                                         'date_updated', 'total_vaccinations', 'persons_vaccinated_1plus_dose',
                                         'total_vaccinations_per100', 'persons_vaccinated_1plus_dose_per100',
                                         'persons_fully_vaccinated', 'persons_fully_vaccinated_per100',
                                         'vaccines_used', 'first_vaccine_date', 'number_vaccines_types_used',
                                         'persons_booster_add_dose', 'persons_booster_add_dose_per100',]]

now_df_vaccination[now_df_vaccination[['country_code', 'country', 'who_region', 
                                       'data_source', 'date_updated']].isna().any(axis = 1)]


# In[49]:


# Store the countries with NaN value
nan_now_df_vaccination = now_df_vaccination[now_df_vaccination[['country_code', 'country', 
                                                                'who_region', 'data_source', 'date_updated']]
                                            .isna().any(axis = 1)].country


# ### Vaccination Metadata

# In[50]:


now_df_vaccination_md.columns


# In[51]:


now_df_vaccination_md['country'] = now_df_vaccination_md['iso3'].apply(lambda x: country_iso3_mapping.get(x))
now_df_vaccination_md['country_code'] = now_df_vaccination_md['country'].apply(lambda x: country_code_mapping.get(x))
now_df_vaccination_md = now_df_vaccination_md[['country_code', 'country', 'vaccine_name', 'product_name', 'company_name',
                                               'authorization_date', 'start_date', 'end_date', 'comment', 'data_source']]

now_df_vaccination_md[now_df_vaccination_md[['country_code', 'vaccine_name']].isna().any(axis = 1)]


# In[52]:


# Store the countries with NaN value
nan_now_df_vaccination_md = now_df_vaccination_md[now_df_vaccination_md[['country_code', 'vaccine_name']]
                                               .isna().any(axis = 1)].country


# ## Match Country Name(s) - 2nd Layer

# In[53]:


print("Matching Country Name(s) - 2nd Layer .....")
print()


# In[54]:


# List of all countries in the python modules
countries = [country.name for country in pycountry.countries]
countries


# In[55]:


# New dictionary for matching values
country_iso3_mapping_new = {}
country_code_mapping_new = {}
    
# Get country name from ISO3
for key, value in country_iso3_mapping.items():
    country_iso3_mapping_new[key] = swap_text(value)

# Get country code from country name
for key in country_code_mapping.keys():
    country_code_mapping_new[swap_text(key)] = country_code_mapping[key]
    
countries = list(country_code_mapping_new.keys())


# ### Search for word with highest similarity

# In[56]:


now_df_daily_cd[now_df_daily_cd['country_code'].isna() == 1]


# ### Check NaN entries (country)

# In[57]:


# Only run this cell for debugging

# for item in [nan_now_df_daily_cd, nan_now_df_latest_cd, nan_now_df_vaccination, nan_now_df_vaccination_md]:
#     for idx, value in enumerate(item):
#         try:
#             print(f"{value} => \t{most_similar(value)} => \t{country_code_mapping_new[most_similar(value)]}")
            
#         except Exception as e:
#             print(e)


# First, we will change the 'Palestinian' name into 'State of Palestinian and Jerusalem', 'Sint Eustatius' into 'Sint Eustatius and Saba Bonaire', and also 'Sint Marteen' into 'Sint Marteen (Dutch part).

# In[58]:


dct1 = dict(zip(nan_now_df_daily_cd.index, nan_now_df_daily_cd.values)) # nan_now_df_daily_cd
dct2 = dict(zip(nan_now_df_latest_cd.index, nan_now_df_latest_cd.values)) # nan_now_df_latest_cd
dct3 = dict(zip(nan_now_df_vaccination.index, nan_now_df_vaccination.values)) # nan_now_df_vaccination
dct4 = dict(zip(nan_now_df_vaccination_md.index, nan_now_df_vaccination_md.values)) # nan_now_df_vaccination_md

# Check if there are any strings which contains the words we want to change
count = 0

"""
0: nan_now_df_daily_cd
1: nan_now_df_latest_cd
2: nan_now_df_vaccination
3: nan_now_df_vaccination_md
"""

change_name = {}
for dct in [dct1, dct2, dct3, dct4]:
    for idx, value in dct.items():       
        try:
            if ('martin' in value.lower()) or ('maarten' in value.lower()) or ('marteen' in value.lower()):
                change_name[value] = 'Sint Marteen (Dutch part)'
                if count == 0:
                    nan_now_df_daily_cd = nan_now_df_daily_cd.drop(idx)
                elif count == 1:
                    nan_now_df_latest_cd = nan_now_df_latest_cd.drop(idx)
                elif count == 2:
                    nan_now_df_vaccination = nan_now_df_vaccination.drop(idx)
                else:
                    nan_now_df_vaccination_md = nan_now_df_vaccination_md.drop(idx)

            elif ('palestinian' in value.lower()) or ('palestine' in value.lower()):
                change_name[value] = 'State of Palestinian and Jerusalem'
                if count == 0:
                    nan_now_df_daily_cd = nan_now_df_daily_cd.drop(idx)
                elif count == 1:
                    nan_now_df_latest_cd = nan_now_df_latest_cd.drop(idx)
                elif count == 2:
                    nan_now_df_vaccination = nan_now_df_vaccination.drop(idx)
                else:
                    nan_now_df_vaccination_md = nan_now_df_vaccination_md.drop(idx)

            elif ('eustatius' in value.lower()) or ('saba' in value.lower()) or ('bonaire' in value.lower()):
                change_name[value] = 'Sint Eustatius and Saba Bonaire'
                if count == 0:
                    nan_now_df_daily_cd = nan_now_df_daily_cd.drop(idx)
                elif count == 1:
                    nan_now_df_latest_cd = nan_now_df_latest_cd.drop(idx)
                elif count == 2:
                    nan_now_df_vaccination = nan_now_df_vaccination.drop(idx)
                else:
                    nan_now_df_vaccination_md = nan_now_df_vaccination_md.drop(idx)
                    
            elif ('kosovo' in value.lower()):
                change_name[value] = 'Kosovo'
                if count == 0:
                    nan_now_df_daily_cd = nan_now_df_daily_cd.drop(idx)
                elif count == 1:
                    nan_now_df_latest_cd = nan_now_df_latest_cd.drop(idx)
                elif count == 2:
                    nan_now_df_vaccination = nan_now_df_vaccination.drop(idx)
                else:
                    nan_now_df_vaccination_md = nan_now_df_vaccination_md.drop(idx)

        except Exception as e:
            print(count, idx, value, e)
            pass
        
    count = count + 1


# In[59]:


now_df_daily_cd['country'] = now_df_daily_cd['country'].apply(lambda x: change_name.get(x) if x 
                                                              in change_name.keys() else x)
now_df_latest_cd['country'] = now_df_latest_cd['country'].apply(lambda x: change_name.get(x) if x 
                                                                in change_name.keys() else x)
now_df_vaccination['country'] = now_df_vaccination['country'].apply(lambda x: change_name.get(x) if x 
                                                                    in change_name.keys() else x)
now_df_vaccination_md['country'] = now_df_vaccination_md['country'].apply(lambda x: change_name.get(x) if x 
                                                                          in change_name.keys() else x)


# ### Match country and country_code values

# Now, we will focus on handling the None, Other, and matching the country - country_code values.

# In[60]:


# Adding extra country code for Kosovo
country_code_mapping_new['Kosovo'] = 'XK'
countries.append('Kosovo')


# In[61]:


# Renew our series which contains of null values
nan_now_df_daily_cd = now_df_daily_cd[now_df_daily_cd[['country_code', 'country']]
                                      .isna().any(axis = 1)]

nan_now_df_latest_cd= now_df_latest_cd[now_df_latest_cd[['country_code', 'country']]
                                       .isna().any(axis = 1)]

nan_now_df_vaccination = now_df_vaccination[now_df_vaccination[['country_code', 'country']]
                                            .isna().any(axis = 1)]

nan_now_df_vaccination_md = now_df_vaccination_md[now_df_vaccination_md[['country_code']]
                                                  .isna().any(axis = 1)]


# We will drop the columns which have None or Other values, because it is impossible to locate the actual value (country) of the missing value.

# In[62]:


# Find and delete None values in nan_now_df_daily_cd
idxes = nan_now_df_daily_cd.index
now_df_daily_cd = drop_none_other(idxes, now_df_daily_cd)

# Find and delete None values in nan_now_df_latest_cd
idxes = nan_now_df_latest_cd.index
now_df_latest_cd = drop_none_other(idxes, now_df_latest_cd)
    
# Find and delete None values in nan_now_df_vaccination
idxes = nan_now_df_vaccination.index
now_df_vaccination = drop_none_other(idxes, now_df_vaccination)
    
# Find and delete None values in nan_now_df_vaccination_md
idxes = nan_now_df_vaccination_md.index
now_df_vaccination_md = drop_none_other(idxes, now_df_vaccination_md)


# At this point, we can be sure that we can do matching between country_code and country. First, we will get the country code from the country name info.

# In[63]:


# Renew our series which contains null values again
nan_now_df_daily_cd = now_df_daily_cd[now_df_daily_cd[['country_code', 'country']]
                                      .isna().any(axis = 1)]

nan_now_df_latest_cd = now_df_latest_cd[now_df_latest_cd[['country_code', 'country']]
                                       .isna().any(axis = 1)]

nan_now_df_vaccination = now_df_vaccination[now_df_vaccination[['country_code', 'country']]
                                            .isna().any(axis = 1)]

nan_now_df_vaccination_md = now_df_vaccination_md[now_df_vaccination_md[['country_code']]
                                                  .isna().any(axis = 1)]


# In[64]:


# Match missing values in nan_now_df_daily_cd
idxes = nan_now_df_daily_cd.index
for idx in idxes:
    now_df_daily_cd = get_code_from_country(idx, now_df_daily_cd)

# Match missing values in nan_now_df_latest_cd
idxes = nan_now_df_latest_cd.index
for idx in idxes:
    now_df_latest_cd = get_code_from_country(idx, now_df_latest_cd)
    
# Match missing values in nan_now_df_vaccination
idxes = nan_now_df_vaccination.index
for idx in idxes:
    now_df_vaccination = get_code_from_country(idx, now_df_vaccination)
    
# Match missing values in nan_now_df_vaccination_md
idxes = nan_now_df_vaccination_md.index
for idx in idxes:
    now_df_vaccination_md = get_code_from_country(idx, now_df_vaccination_md)


# Now, we will do the opposite, which is to get the country name from the country code info.

# In[65]:


# Renew our series which contains null values again
nan_now_df_daily_cd = now_df_daily_cd[now_df_daily_cd[['country_code', 'country']]
                                      .isna().any(axis = 1)]

nan_now_df_latest_cd = now_df_latest_cd[now_df_latest_cd[['country_code', 'country']]
                                       .isna().any(axis = 1)]

nan_now_df_vaccination = now_df_vaccination[now_df_vaccination[['country_code', 'country']]
                                            .isna().any(axis = 1)]

nan_now_df_vaccination_md = now_df_vaccination_md[now_df_vaccination_md[['country_code']]
                                                  .isna().any(axis = 1)]


# In[66]:


# Match missing values in nan_now_df_daily_cd
idxes = nan_now_df_daily_cd.index
for idx in idxes:
    now_df_daily_cd = get_country_from_code(idx, now_df_daily_cd)

# Match missing values in nan_now_df_latest_cd
idxes = nan_now_df_latest_cd.index
for idx in idxes:
    now_df_latest_cd = get_country_from_code(idx, now_df_latest_cd)
    
# Match missing values in nan_now_df_vaccination
idxes = nan_now_df_vaccination.index
for idx in idxes:
    now_df_vaccination = get_country_from_code(idx, now_df_vaccination)
    
# Match missing values in nan_now_df_vaccination_md
idxes = nan_now_df_vaccination_md.index
for idx in idxes:
    now_df_vaccination_md = get_country_from_code(idx, now_df_vaccination_md)


# Now, we only need to match our country name format with the ones in the dictionary.

# In[67]:


try:
    # Daily Case and Death
    # Pair of words with highest similarity for each country
    unique_countries = {country: most_similar(country) for country in now_df_daily_cd['country'].unique()}
    
    # Set the value
    now_df_daily_cd['country'] = now_df_daily_cd['country'].apply(lambda x: unique_countries.get(x))
    
    # Latest Case and Death
    now_df_latest_cd['country'] = now_df_latest_cd['country'].apply(most_similar)
    
    # Vaccination Data
    now_df_vaccination['country'] = now_df_vaccination['country'].apply(most_similar)
    
    # Vaccination Metadata
    now_df_vaccination_md['country'] = now_df_vaccination_md['country'].apply(most_similar)
    
    print("Successfully manipulated the Data.")
    print("================================")
    print()
    
except Exception as e:
    print(e)
    pass


# ## Add Extra Feature(s)

# For now, there will be only one feature we will add, which is the last update date in the now_df_latest_cd dataframe to give info when exactly the 'last 7 weeks' features are referred to.

# In[68]:


print("Adding Features .....")
print()


# In[69]:


# Daily Case and Death
# -


# In[70]:


# Latest Case and Death
# Fetch the max reported date for each country
max_date = now_df_daily_cd.groupby('country')[['date_reported']].max().reset_index(level = 0)
country_idx = max_date['country']
country_date = max_date['date_reported']

# Make it into dictionary
max_date = dict(zip(country_idx, country_date))
max_date['Global'] = max(max_date.values())

now_df_latest_cd['last_reported'] = now_df_latest_cd['country'].apply(lambda x: max_date.get(x))


# In[71]:


# Vaccination
# -


# In[72]:


# Vaccination Metadata
# -


# In[73]:


now_df_vaccination_md[['country', 'country_code']]


# # Check Format Data

# Check if the yesterday's data format is different than today's data format, so we can adjust quickly.

# In[74]:


print("================================")
print("Checking Data Format .....")
print()

# Function to Check features
def check_features():
    count_features = 0
    for item in now_df_daily_cd.columns:
        if item not in yesterday_df_daily_cd.columns:
            count_features += 1
            
    for item in now_df_latest_cd.columns:
        if item not in yesterday_df_latest_cd.columns:
            count_features += 1
            
    for item in now_df_vaccination.columns:
        if item not in yesterday_df_vaccination.columns:
            count_features += 1
            
    for item in now_df_vaccination_md.columns:
        if item not in yesterday_df_vaccination_md.columns:
            count_features += 1

    if count_features != 0:
        print(f"Feature is not same. Adjustment needed in today's data features.")
        print()
        
        # Inconsistent Format Error
        inconsistent_format = logging.handlers.SMTPHandler(mailhost = ("smtp.example.com", 25),
                                                           fromaddr = "nabilfarras923@gmail.com", 
                                                           toaddrs = "nabilfarras923@gmail.com",
                                                           subject = u"COVID-19 Automation Inconsistent Data Format!")
        
        logger = logging.getLogger()
        logger.addHandler(inconsistent_format)
        logger.exception('Unhandled Exception')
        
    else:
        return "Pass. Data is ready to be imported."
        print()


# In[75]:


# # Check country rows, is yesterday's data a subset of today's data
# def check_subset():
#     count_subset = 0
#     for item in now_df_daily_cd['country'].unique():
#         if item not in yesterday_df_daily_cd['country'].unique():
#             count_subset += 1
            
#     for item in now_df_latest_cd['country'].unique():
#         if item not in yesterday_df_latest_cd['country'].unique():
#             count_subset += 1
            
#     for item in now_df_vaccination['country'].unique():
#         if item not in yesterday_df_vaccination['country'].unique():
#             count_subset += 1
            
#     for item in now_df_vaccination_md['country'].unique():
#         if item not in yesterday_df_vaccination_md['country'].unique():
#             count_subset += 1

#     if count_subset != 0:
#         print(f"Warning! Today's data is not a subset of yesterday's data.")
        
#         # Not a Subset Error
#         not_a_subset = logging.handlers.SMTPHandler(mailhost = ("smtp.example.com", 25),
#                                                     fromaddr = "nabilfarras923@gmail.com", 
#                                                     toaddrs = "nabilfarras923@gmail.com",
#                                                     subject = u"COVID-19 Automation Not a Subset!")
        
#         logger = logging.getLogger()
#         logger.addHandler(not_a_subset)
#         logger.exception('Unhandled Exception')
        
#     else:
#         return "Pass. Data is ready to be imported."
#         print()


# In[76]:


# df_dict = {'yesterday_df_daily_cd': 'now_df_daily_cd',
#           'yesterday_df_latest_cd': 'now_df_latest_cd',
#           'yesterday_df_vaccination': 'now_df_vaccination',
#           'yesterday_df_vaccination_md': 'now_df_vaccination_md'}

if cond == True: # We will check today's data compared to yesterday's data
    check_features()
#     check_subset()
    
else:
    print("There is no data recorded yet to compare with.")
    print()

print("Data Format have been checked.")
print("================================")
print()


# # Feature Engineering

# In[77]:


# Create the cummulative cases and deaths for each country
today_cummulative_country = now_df_daily_cd.groupby(['country', 'who_region'])[['date_reported', 
                                                                                'cumulative_cases', 
                                                                                'cumulative_deaths']] \
                            .agg({'date_reported': 'max',
                                  'cumulative_cases': 'max', 
                                  'cumulative_deaths': 'max'}).reset_index()

today_cummulative_country


# In[78]:


# Create the cummulative cases and deaths for each date
today_cummulative = now_df_daily_cd.groupby('date_reported')[['cumulative_cases', 
                                                              'cumulative_deaths']] \
                    .agg({'cumulative_cases': 'sum', 
                          'cumulative_deaths': 'sum'}).reset_index()

today_cummulative


# In[79]:


# Create the cummulative cases and deaths for each region
today_cummulative_region = now_df_daily_cd.groupby('who_region')[['date_reported', 
                                                                   'cumulative_cases', 
                                                                   'cumulative_deaths']] \
                            .agg({'date_reported': 'max',
                                  'cumulative_cases': 'max', 
                                  'cumulative_deaths': 'max'}).reset_index()

today_cummulative_region


# In[80]:


# Create the cummulative vaccination info for each country
cumulative_vaccination_country = now_df_vaccination.copy()
cumulative_vaccination_country = cumulative_vaccination_country[['country_code', 'country', 'who_region', 'date_updated', 
                                                                 'total_vaccinations', 'persons_vaccinated_1plus_dose',
                                                                 'total_vaccinations_per100', 
                                                                 'persons_vaccinated_1plus_dose_per100',
                                                                 'persons_fully_vaccinated', 
                                                                 'persons_fully_vaccinated_per100',
                                                                 'vaccines_used', 'first_vaccine_date', 
                                                                 'number_vaccines_types_used',
                                                                 'persons_booster_add_dose', 
                                                                 'persons_booster_add_dose_per100']]

# cumulative_vaccination_country['date_updated'] = dt.date.today()

cumulative_vaccination_country = cumulative_vaccination_country.reset_index(drop = True)
cumulative_vaccination_country


# In[81]:


# Create the cummulative vaccination info for each region
cumulative_vaccination_region = now_df_vaccination.copy()
cumulative_vaccination_region = cumulative_vaccination_region[['country_code', 'country', 'who_region', 'date_updated', 
                                                               'total_vaccinations', 'persons_vaccinated_1plus_dose',
                                                               'total_vaccinations_per100', 
                                                               'persons_vaccinated_1plus_dose_per100',
                                                               'persons_fully_vaccinated', 
                                                               'persons_fully_vaccinated_per100',
                                                               'vaccines_used', 'first_vaccine_date', 
                                                               'number_vaccines_types_used',
                                                               'persons_booster_add_dose', 
                                                               'persons_booster_add_dose_per100']]

# cumulative_vaccination_region['date_updated'] = dt.date.today()

cumulative_vaccination_region = cumulative_vaccination_region.groupby('who_region') \
                                                             [['date_updated', 
                                                               'total_vaccinations', 'persons_vaccinated_1plus_dose',
                                                               'total_vaccinations_per100', 
                                                               'persons_vaccinated_1plus_dose_per100',
                                                               'persons_fully_vaccinated', 
                                                               'persons_fully_vaccinated_per100']] \
                                .agg({'date_updated': 'max',
                                      'total_vaccinations': 'sum', 
                                      'persons_vaccinated_1plus_dose': 'sum',
                                      'total_vaccinations_per100': 'mean', 
                                      'persons_vaccinated_1plus_dose_per100': 'mean',
                                      'persons_fully_vaccinated': 'sum', 
                                      'persons_fully_vaccinated_per100': 'mean'}).reset_index()

cumulative_vaccination_region


# # Convert DataFrame into SQL Table in Local DB

# ## Delete Existing Content in Table(s)

# In[82]:


print("================================")
print("Deleting Existing Content in Every Tables .....")
print()

sqlEngine = sql.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}" \
				.format(host = hostname,
                db = dbname,
                user = uname,
                pw = pwd))

dbConnection    = sqlEngine.connect()

Base = declarative_base()
metadata = sql.MetaData()
insp = sql.inspect(sqlEngine)

for table in insp.get_table_names():
    truncate_query = sql.text(f"TRUNCATE TABLE {table}")
    dbConnection.execution_options(autocommit = True).execute(truncate_query)
    
print("No content remained in the local DB.")
print("================================")
print()


# In[83]:


# print("================================")
# print("Deleting Existing Tables .....")
# print()

# Base = declarative_base()
# metadata = sql.MetaData()
# insp = sql.inspect(sqlEngine)

# def drop_table(tbl_name, engine = sqlEngine):
    
#     # Since we are already connected to the local db, we can skip to the next step
#     # which is dropping all the previous day' tables
#     Base.metadata.drop_all(engine)
#     metadata.reflect(bind = engine)
#     table = metadata.tables[tbl_name]
#     if table is not None:
#         Base.metadata.drop_all(engine, [table], checkfirst = True)
        
# for table in insp.get_table_names():
#     drop_table(table)

# print("No table remained in the local DB.")
# print("================================")
# print()


# ## Upload DataFrame to 'COVID-19' Local DB

# In[84]:


print("================================")
print("Uploading DataFrame .....")
print()

dfs = [now_df_daily_cd, now_df_latest_cd, now_df_vaccination, now_df_vaccination_md,
       today_cummulative_country, today_cummulative, today_cummulative_region, 
       cumulative_vaccination_country, cumulative_vaccination_region] # List of DataFrames

tbls = ['daily_case_death', 'latest_case_death', 'daily_vaccination', 'vaccination_md',
        'today_cummulative_country', 'today_cummulative', 'today_cummulative_region', 
        'cumulative_vaccination_country', 'cumulative_vaccination_region'] # List of table names

for i in range(len(dfs)):
    try:
        dfs[i].to_sql(tbls[i], sqlEngine, if_exists = "append", index = False)
        print("Successfully uploaded.")
        print()
        
    except Exception as e:
        print(f"Upload Fail.")
        print(f"Type:", e)
        print()
        to_sql_error = logging.handlers.SMTPHandler(mailhost = ("smtp.example.com", 25),
                                                    fromaddr = "nabilfarras50@gmail.com", 
                                                    toaddrs = "nabilfarras923@gmail.com",
                                                    subject = u"COVID-19 Automation to SQL Error ({})!".format(dfs[i]))

        logger = logging.getLogger()
        logger.addHandler(to_sql_error)
        logger.exception('Unhandled Exception')

print("All process ended.")
print("================================")

