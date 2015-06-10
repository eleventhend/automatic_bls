# Automatic BLS!
Making BLS data accessible since May 2015.

##Automatic_bls.py
Does the following:

1. Query api based on series read from csv files
2. Send api data to sql database, updating and archiving deprecated data

1 - Empties out the incubator table
2 - Sends requests to BLS API for all prefixes listed in prefix.csv
  2.1 - Takes series components from respective local folders
  2.2 - Outputs to SQL incubator table using sqlalchemy engine
3 - Inserts the incubator table into the fact table
  3.1 - Sends deprecated data to archive table
  3.2 - Updates fact table with new data

######Note that debugging prints & timers are still in to gauge how the API is handling requests & timeouts

#####Requires command line argument of desired database prefix. Current arguments include:

`$ python automatic_bls.py LA`

`$ python automatic_bls.py SM`

#####Future arguments will include:

`$ python automatic_bls.py EN`

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


##dimension_maker.py
Does the following:

1. Constructs dimension tables for: series, measure, geographic, and sector codes.
2. Fills these dimension tables with all available codes and descriptions for all prefixes in the prefix file.


###Requirements
#####sqlalchemy
http://docs.sqlalchemy.org/en/latest/intro.html#installation-guide

#####pandas
http://pandas.pydata.org/pandas-docs/stable/install.html

#####sql_engine.txt
The sqlalchemy address to your sql database:
http://docs.sqlalchemy.org/en/latest/core/engines.html