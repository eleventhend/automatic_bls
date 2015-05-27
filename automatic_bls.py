"""
automatic_bls.py:
1. Query api based on series read from csv files
2. Send api data to sql database, updating and archiving deprecated data
"""

#Copyright (C) 2015 Jeff Ferguson <jferguson51 AT gmail DOT com>

#This program is free software; you can redistribute it and/or
#modify it under the terms of the GNU General Public License
#as published by the Free Software Foundation; either version 2
#of the License, or (at your option) any later version.

#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.

#You should have received a copy of the GNU General Public License
#along with this program; if not. If not, see <http://www.gnu.org/licenses/>.

import sys
import time
from datetime import date
import csv

import json
from urllib2 import Request, urlopen

from sqlalchemy import create_engine
import pandas as pd
from bls import get_series

def series_tupler(prefix, seasonal, geo, measure, sector=""):
    """
    takes a pandas dataframe and returns a tuple of tuples
        with the innermost tuples containing series IDs at
        maximum size for the API (50)
    """
    x_ser = ()
    for index, row in geo.iterrows():
        x_ser = x_ser + ((prefix + seasonal + row['area_code'] + measure),)
    series = [x_ser[i:i+49] for i in range(0, len(x_ser), 50)]
    return series

def api_to_sql(series, api_key, engine, start_year, end_year):
    """
    outputs to a sql database a fact table based on input of
        a tuple of tuples of series IDs to fetch and a desired date range.

    **TODO: Improve error handling by:
        Determining exactly which codes are breaking
        Retrying broken 50 code series (minus broken codes)
    """
    try:
        df, resp = get_series(series, api_key, start_year, end_year)
    except KeyError:
        with open('errors.txt', 'a') as errorfile:
            errorfile.write('\n'.join(series))
            errorfile.write('\n' + api_key)
            errorfile.write('\n' + start_year)
            errorfile.write('\n' + end_year + '\n')
        try:
            print "let's check the api's response..."
            print resp
        except:
            "No response returned (KeyError)!"
        return
    except:
        print "There was an unknown error!"
        e = sys.exc_info()[0:2]
        with open ('type_errors.txt', 'a') as te_file:
            te_file.write("Error: %s" % (e,))
        try:
            print resp
        except:
            print "No response returned (Unknown, check type_errors file)!"
        return
    for k in range (1, len(df.columns)):
        df_sql = dataframe_sequencer(df, k)
        pd.DataFrame.to_sql(df_sql, con=engine, name='incubator',
            if_exists='append', index=False)
    return

def dataframe_sequencer(df, i):
    """
    takes a dataframe of raw api data and
        transforms it into a fact-table friendly format
    """
    df_sql = pd.DataFrame({'date':[], 'SeriesID':[], 'data':[]})
    df_sql['date'] = df.index
    df_sql.index = df.index
    df_sql['data'] = df[df.columns[i]]
    df_sql['SeriesID'] = df.columns[i]
    return df_sql

def data_extractor(
    prefix, seasonal, geo, measure_code, api_key, engine, startyear, endyear):
    """
    Uses the above functions to create batches of series IDs, sending them
        to the BLS api, and passing the returned data to the Atlas SQL server.
    """
    for index, row1 in seasonal.iterrows():
        for index, row2 in measure_code.iterrows():
            series=series_tupler(
                prefix, row1['seasonal_code'], geo, row2['measure_code'])
            for i in range(0, len(series)):
                api_to_sql(series[i], api_key, engine, startyear, endyear)
    return

print "Welcome to Atlas automated Bureau of Labor Statistics API dredger v1.0"

with open ('api_key.txt', 'r') as f:
    api_key = f.read()
with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()

engine = create_engine(engine_address)
startyear = str(date.today().year - 1)
endyear = str(date.today().year)
pre_df = pd.DataFrame.from_csv("prefix.csv")
pre_df['prefix'] = pre_df.index

"""
1 - Empties out the incubator table
2 - Sends requests to BLS API for all prefixes listed in prefix.csv
    2.1 - Takes series components from respective local folders
    2.2 - Outputs to SQL incubator table using sqlalchemy engine
3 - Inserts the incubator table into the fact table
    3.1 - Sends deprecated data to archive table
    3.2 - Updates fact table with new data
"""

engine.execute("CREATE TABLE IF NOT EXISTS incubator (`SeriesID` varchar(63),"
    " `data` float, `date` datetime)")
engine.execute("TRUNCATE TABLE incubator")

for x in range(0, len(pre_df.index), 1):
    p = pre_df['prefix'].iloc[x]
    s_df = pd.read_csv(p + "/seasonal_codes.csv")
    geo_df = pd.read_csv(p + "/geo_codes.csv")
    mc_df = pd.read_csv(p + "/measure_codes.csv",
        converters={'measure_code': lambda x: str(x)})
    data_extractor(pre_df['prefix'].iloc[x], s_df, geo_df, mc_df, api_key, engine, 
        startyear, endyear)

engine.execute("CREATE TABLE IF NOT EXISTS fact (`SeriesID` varchar(63),"
    " `data` float, `date` datetime)")
engine.execute("CREATE TABLE IF NOT EXISTS archive (`SeriesID` varchar(63),"
    " `data` float, `date` datetime)")
engine.execute("INSERT INTO archive SELECT f.`SeriesID`, f.`data`, f.`date`"
    " FROM fact as f JOIN incubator AS i ON f.`SeriesID` = i.`SeriesID` AND"
    " f.`date` = i.`date` AND f.`data` != i.`data`")

"""
----Not sure how to run this once & never again----
adding unique constraint for incubator/fact insert into
"""
#engine.execute("ALTER TABLE fact ADD CONSTRAINT fact_UQ"
#    " UNIQUE(`SeriesID`, `date`)")

engine.execute("INSERT INTO fact (`SeriesID`, `data`, `date`) SELECT"
    " i.`SeriesID`, i.`data`, i.`date` FROM incubator AS i ON DUPLICATE"
    " KEY UPDATE `data` = VALUES(`data`)")

"""incubator deduping queries...may not be necessary, are slow"""
#engine.execute("CREATE TABLE dedupe as SELECT * FROM incubator WHERE 1"
#    " GROUP BY `SeriesID`, `data`, `date`")
#engine.execute("DROP TABLE incubator")
#engine.execute("RENAME TABLE dedupe TO incubator")

engine.execute("DROP TABLE incubator")

print "complete! excelsior!"
