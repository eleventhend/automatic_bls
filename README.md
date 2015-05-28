# Automatic BLS!
#### Making BLS data accessible since May 2015.

Automatic_bls.py does the following:

1. Query api based on series read from csv files
2. Send api data to sql database, updating and archiving deprecated data

######Note that debugging prints & timers are still in to gauge how the API is handling requests & timeouts

#####Requires command line argument of desired database prefix. Current arguments include:

$ python automatic_bls.py LA

$ python automatic_bls.py SM

#####Future arguments will include:

$ python automatic_bls.py EN

###Requirements

#####sqlalchemy
http://docs.sqlalchemy.org/en/latest/intro.html#installation-guide

#####pandas
http://pandas.pydata.org/pandas-docs/stable/install.html

#####BLS Python Module
Developed by Oliver Sherouse, available here:
https://github.com/OliverSherouse/bls

#####api_key.txt
Your BLS api key, available here:
http://data.bls.gov/registrationEngine/

#####sql_engine.txt
The sqlalchemy address to your sql database:
http://docs.sqlalchemy.org/en/latest/core/engines.html
