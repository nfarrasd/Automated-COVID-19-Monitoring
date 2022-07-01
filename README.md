# Automated COVID-19 Monitoring

This is an individual side-project made by Moch Nabil Farras Dhiya (me) in June 2022, 
as a mean to implement my automation and ETL process using Python and SQL.
Final data visualization on COVID-19 data from the ETL process is also given in this project using Tableau.

# Dashboard
The dashboard made can be seen here: https://public.tableau.com/app/profile/moch.nabil.farras.dhiya

# Dataset
The data used in this project is obtained from WHO official Website, which can be accessed here: https://covid19.who.int/data

# Steps
1.   Extract data from WHO official Website
2.   Transform the data (making sure it is usable and consistent)
3.   Upload the final data to Local MySQL DB
4.   Connect Tableau with Local MySQL DB
5.   Visualize the data for end-users by Tableau
6.   Automate the whole process using Windows Task Scheduler :D (so that we only need to conduct daily monitoring on the Tableau dashboard for business purposes)

**NOTE**:
1. In this project, I use on-site database in order to minimize the cost as Cloud Databases are mostly paid.
2. For simplicity, there is one step that I skipped (i.e. matching three features: country, country code, and ISO3; in which I only did for country and country code).
But the steps are more or less the same, its just that the if else branch will be even more complex.

# Bibliography
[1] https://blog.paperspace.com/implementing-levenshtein-distance-word-autocomplete-autocorrect/
