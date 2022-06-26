"""
This file contains function that is used in order to
clean the data extracted in the 'Automated COVID-19 Monitoring'
by Moch Nabil Farras Dhiya (10120034)

The dataset used in this project is obtained from WHO official website: https://covid19.who.int/data
"""

# Connect to local
import os

# Importing and transforming file
import pandas as pd

# Data manipulation
import numpy as np
import re # Cleaning texts
import datetime as dt # Datetime manipulation
import pycountry # List of countries

# Notify is there is an error to email
import logging
import logging.handlers


# 1
# Lower the words, replace any symbols except ' with _, and delete any space
def remove_symbols(text):
    # Deleting extra spaces and lowering the char, as well as removing ' symbol
    replacement = {"`": "",
                   "'": "",
                   '"': ""}

    temp_text = re.sub(' {2,}', ' ', text.lower().strip().replace("`", "").replace('"',""))

    return re.sub("[^0-9a-zA-Z]+", "_", temp_text)


# 2
# Get only the alphabetical
def get_alphabet(text):
    arr = re.findall(r'[A-Za-z]+', text)
    return ''.join(arr)


# 3
# Parse all the date format in case of different format from multiple sources
# Note: This function is used when data from multiple sources are used. For this case,
# where data is coming from single source(WHO), it most likely will not be of use.
def date_parsing(text):    
    fmts = ('%Y', '%b %d, %Y','%b %d,%Y', '%B %d, %Y', '%B %d %Y',
            '%d %b, %Y', '%d %b,%Y', '%d %b %Y', '%d %B %Y', '%b %d, %Y',
            '%b %d %Y', '%b %d,%Y', '%Y %b %d', '%Y %B %d', '%d/%m/%Y', '%d/%m/%y',
            '%b %Y', '%B%Y', '%b %d,%Y', '%Y/%m/%d', '%y/%m/%d', '%Y %b',
            '%Y, %b %d', '%Y, %B %d', '%Y, %d %b', '%Y, %d %B', '%Y %d %b', '%Y %d %B')
    
    for fmt in fmts:
        try:
            temp_text = re.sub(' {2,}', ' ', text.strip().replace('-', '/'))
            parsed = dt.datetime.strptime(temp_text, fmt)
            return parsed

        except Exception as e:
            return text


# 4
# Swap the word position if contains comma
def swap_text(text):
    if ',' in text:
        return (text.split(',')[1] + ' ' + text.split(',')[0]).strip()
    
    else:
        return text


# 5
# Levenshtein distance to calculate the distance between 2 words (checking 2 words similarity)
def levenshteinDistanceDP(token1, token2):
    distances = np.zeros((len(token1) + 1, len(token2) + 1))

    for t1 in range(len(token1) + 1):
        distances[t1][0] = t1

    for t2 in range(len(token2) + 1):
        distances[0][t2] = t2
        
    a = 0
    b = 0
    c = 0
    
    for t1 in range(1, len(token1) + 1):
        for t2 in range(1, len(token2) + 1):
            if (token1[t1-1] == token2[t2-1]):
                distances[t1][t2] = distances[t1 - 1][t2 - 1]
            else:
                a = distances[t1][t2 - 1]
                b = distances[t1 - 1][t2]
                c = distances[t1 - 1][t2 - 1]
                
                if (a <= b and a <= c):
                    distances[t1][t2] = a + 1
                elif (b <= a and b <= c):
                    distances[t1][t2] = b + 1
                else:
                    distances[t1][t2] = c + 1

    return distances[len(token1)][len(token2)]


# 6
# New dictionary for matching values
country_iso3_mapping_new = {}
country_code_mapping_new = {}
    
# Get country name from ISO3
country_iso3_mapping = {country.alpha_3: country.name for country in pycountry.countries}

# Get country code from country name
country_code_mapping = {country.name: country.alpha_2 for country in pycountry.countries}

for key, value in country_iso3_mapping.items():
    country_iso3_mapping_new[key] = swap_text(value)

# Get country code from country name
for key in country_code_mapping.keys():
    country_code_mapping_new[swap_text(key)] = country_code_mapping[key]
    
countries = list(country_code_mapping_new.keys())
# Find the word with the highest similarity
def most_similar(text):
    idx = 0
    score = [levenshteinDistanceDP(text, countries[i]) for i in range(len(countries))]
    
    # Word with the most similarity has the least score
    minv = min(score)
    
    # As the index of country in both list are the same, this is acceptable
    return countries[score.index(minv)] 


# 7
# Drop dataframe which contains 2 or more undersired values in different cols
def drop_none_other(idxes, df):
    drop_list = [] # List of indexes that will be dropped
    red_flag = ['None', 'none', 'Other', 'other', 'Others', 'others', 'no info', 'No info', 'No Info']
    
    for idx in idxes:
        if ((df.iloc[idx]['country_code'] == None) or (df.iloc[idx]['country_code'] in red_flag)) \
            and ((df.iloc[idx]['country'] == None) or (df.iloc[idx]['country'] in red_flag)):

                # If there are no country_code nor country info, then we cant do anything about it
                # and just drop the row
                drop_list.append(idx)

    return df.drop(drop_list).reset_index(drop = True)


# 8
# Get the country info from the country_code info
def get_code_from_country(idx, df):
    if df.iloc[idx]['country_code'] not in country_code_mapping_new.values():
        
            value = df.iloc[idx]['country']
            
            # Change the values
            # print(idx, value, most_similar(value), country_code_mapping_new[most_similar(value)])
            df.at[idx, 'country_code'] = country_code_mapping_new[most_similar(value)]
            return df
        
    else:
        return df


# 9
# Get key from value
def get_key_from_value(dct, value):
    
    # As country_code and country are unique, we can do this
    return ''.join([key for key, val in dct.items() if value == val])


# 10
# Get country info from its country code
def get_country_from_code(idx, df):
    if df.iloc[idx]['country_code'] in country_code_mapping_new.values() \
        and df.iloc[idx]['country'] not in country_code_mapping_new.keys():

            code = df.iloc[idx]['country_code']
            
            # Change the values
            # print(idx, key, most_similar(value), get_country_from_code(key))
            df.at[idx, 'country'] = get_key_from_value(country_code_mapping_new, code)
            return df
        
    else:
        return df
