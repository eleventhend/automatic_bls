# Automatic BLS!
Making BLS data accessible since May 2015.

##How to use
1. Download and install all required packages (see below)
2. Download this repository
3. Register with the Bureau of Labor Statistics and acquire an API Key (free)
  1. Create a .txt file containing the API key in this repository's directory
  2. Create a .txt file containing your SQLAlchemy engine address (see below for instructions)
4. Run dimension_maker.py
5. Run automatic_bls.py for each desired database

##dimension_maker.py
Does the following:

1. Constructs dimension tables for: series, measure, geographic, and sector codes.
2. Fills these dimension tables with all available codes and descriptions for all prefixes in the prefix file.
3. Creates a unique ID for each entry using the MySQL autoincrement function.


###Requirements
#####SQLAlchemy
http://docs.sqlalchemy.org/en/latest/intro.html#installation-guide

#####pandas
http://pandas.pydata.org/pandas-docs/stable/install.html

#####numpy
http://www.numpy.org/

####In directory:

#####sql_engine.txt
The SQLAlchemy address to your sql database:
http://docs.sqlalchemy.org/en/latest/core/engines.html


##Automatic_bls.py
Does the following:

1. Creates necessary SQL tables: Incubator, Fact, Archive
2. Empties out the incubator table
3. Sends requests to BLS API for all prefixes listed in prefix.csv
  1. Takes series components from respective local folders
  2. Outputs to SQL incubator table using SQLAlchemy engine
4. Cross-populates foreign keys from existing dimension tables
5. Inserts the incubator table into the fact table
  1. Sends deprecated data to archive table
  2. Updates fact table with new data

#####Requires command line argument of desired database prefix. Current arguments include:

`$ python automatic_bls.py LA`
`$ python automatic_bls.py SM`

#####Optional arguments:

`--start_year [1980-2015 default=last year]`
`--end_year [1980-2015 default=current year]`

#####Future arguments will include:

`$ python automatic_bls.py EN`


###Requirements

#####SQLAlchemy
http://docs.sqlalchemy.org/en/latest/intro.html#installation-guide

#####pandas
http://pandas.pydata.org/pandas-docs/stable/install.html

#####BLS Python Module
Developed by Oliver Sherouse, available here:
https://github.com/OliverSherouse/bls

####In directory:

#####api_key.txt
Your BLS api key, available here:
http://data.bls.gov/registrationEngine/

#####sql_engine.txt
The SQLAlchemy address to your sql database:
http://docs.sqlalchemy.org/en/latest/core/engines.html
