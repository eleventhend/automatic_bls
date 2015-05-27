# Automatic BLS!
##### Making BLS data accessible since May 2015.

Automatic_bls.py does the following:

1. Query api based on series read from csv files
2. Send api data to sql database, updating and archiving deprecated data

######Will eventually command line argument of desired database prefix
e.g. $ python automatic_bls.py LA


###Requirements (not included here):

#####sqlalchemy
http://docs.sqlalchemy.org/en/latest/intro.html#installation-guide

#####pandas
http://pandas.pydata.org/pandas-docs/stable/install.html

#####BLS Python Module
Developed by Oliver Sherouse, available here:
https://github.com/OliverSherouse/bls

#####api_key.txt
.txt file containing your BLS api key, available here:
http://data.bls.gov/registrationEngine/

#####sql_engine.txt
.txt file containing the sqlalchemy address to your sql database
See: http://docs.sqlalchemy.org/en/latest/core/engines.html

banana banana banana
