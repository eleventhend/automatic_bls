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
#along with this program; if not, see <http://www.gnu.org/licenses/>.

import time
from datetime import date
import csv
import argparse

import json
from urllib2 import Request, urlopen

from sqlalchemy import create_engine
import pandas as pd
from bls import get_series

def api_to_sql(series, api_key, engine, start_year, end_year):
    """
    Outputs to a sql database a fact table based on an input of
        a tuple of tuples of SeriesIDs to fetch and a desired date range.

    **TODO: Improve error handling by:
        Determining exactly which codes are breaking
        Retrying broken 50 code series (minus broken codes)
    """
    try:
        df = get_series(series, api_key, start_year, end_year)
        for k in range (1, len(df.columns)):
            df_sql = dataframe_sequencer(df, k)
            pd.DataFrame.to_sql(df_sql, con=engine, name='incubator',
                if_exists='append', index=False)
        print "Query successful!"
        time.sleep(5)
    except Exception as e:
        with open('errors.txt', 'a') as errorfile:
            errorfile.write('\n'.join(series))
            errorfile.write('\n' + api_key)
            errorfile.write('\n' + start_year)
            errorfile.write('\n' + end_year + '\n')
        with open ('type_errors.txt', 'a') as te_file:
            te_file.write("Error: %s \n" % (e,))
        print "Query failed, waiting 5 seconds"
        time.sleep(5)
        return
    return

def dataframe_sequencer(df, i):
    """
    Takes a dataframe of raw api data and transforms it into the format
        required by the fact table (3 columns, SeriesID/data/date)
    """
    df_sql = pd.DataFrame({'date':[], 'SeriesID':[], 'data':[]})
    df_sql['date'] = df.index
    df_sql.index = df.index
    df_sql['data'] = df[df.columns[i]]
    df_sql['SeriesID'] = df.columns[i]
    return df_sql

def data_extractor(prefix, seasonal, geo, m_c, api_key, engine, startyear, 
    endyear, sector = pd.DataFrame()):
    """
    Uses previously defined functions to create batches of SeriesIDs, sends 
        them to the BLS api, and inserts the returned data to an incubator
        table in the SQL server.
    """
    try:
        sector['sector_code'].iloc[0]
    except:
       sector = pd.DataFrame(index = [0], columns = ['sector_code'])
       sector = sector.fillna('')
    allseries = ()
    for x in range(0, len(seasonal.index), 1):
        for y in range(0, len(m_c.index), 1):
            for z in range(0, len(sector.index), 1):
                for i in range(0, len(geo.index), 1):
                    ser_concat = (prefix + seasonal['seasonal_code'].iloc[x] + 
                        geo['area_code'].iloc[i] + 
                        sector['sector_code'].iloc[z] + 
                        m_c['measure_code'].iloc[y])
                    allseries = allseries + (ser_concat,)
    series = [allseries[i:i+49] for i in range(0, len(allseries), 50)]
    for i in range(0, len(series)):
        api_to_sql(series[i], api_key, engine, startyear, endyear)
    return

#Retrieves a single command line argument which determines the target database
parser = argparse.ArgumentParser()
parser.add_argument("Prefix", help = "Requires the prefix of the desired \
    database as an argument")
args = parser.parse_args()

#Get API key and SQL engine address (secure info)
with open ('api_key.txt', 'r') as f:
    api_key = f.read()
with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()

#Setup sqlalchemy engine; set start & end year to last year and this year
engine = create_engine(engine_address)
startyear = str(date.today().year - 1)
endyear = str(date.today().year)

#set up empty incubator table
engine.execute("CREATE TABLE IF NOT EXISTS incubator (`SeriesID` varchar(63), \
    `data` float, `date` datetime)")
engine.execute("TRUNCATE TABLE incubator")

#Get series code components from CSV
s_df = pd.read_csv(args.Prefix + "/seasonal_codes.csv")
geo_df = pd.read_csv(args.Prefix + "/geo_codes.csv",
    converters={'area_code': lambda x: str(x)})
mc_df = pd.read_csv(args.Prefix + "/measure_codes.csv",
    converters={'measure_code': lambda x: str(x)})

#If there is a sector file for this database use it; otherwise don't
try:
    sector_df = pd.read_csv(args.Prefix + "/sector_codes.csv",
        converters={'sector_code': lambda x: str(x)})
    data_extractor(args.Prefix, s_df, geo_df, mc_df, api_key, engine, 
        startyear, endyear, sector_df)
except:
    data_extractor(args.Prefix, s_df, geo_df, mc_df, api_key, engine, 
        startyear, endyear)

#Checks that fact and archive tables exist
#Inserts changed fact entries into the archive table
engine.execute("CREATE TABLE IF NOT EXISTS fact (`SeriesID` varchar(63), \
    `data` float, `date` datetime)")
engine.execute("CREATE TABLE IF NOT EXISTS archive (`SeriesID` varchar(63), \
    `data` float, `date` datetime, `archivedate` \
    TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
engine.execute("INSERT INTO archive (`SeriesID`, `data`, `date`, \
    `archivedate`) SELECT f.`SeriesID`, f.`data`, f.`date`, \
    CURRENT_TIMESTAMP FROM `fact` as f JOIN `incubator` AS i ON \
    f.`SeriesID` = i.`SeriesID` AND f.`date` = i.`date` AND \
    f.`data` != i.`data`")

#Creates a sql procedure which checks whether a specific unique index exists,
#then creates it in the desired table if it does not.
engine.execute("DROP PROCEDURE IF EXISTS atlas_bls.`CreateIndex`")
engine.execute("""CREATE PROCEDURE atlas_bls.`CreateIndex`
(
    g_db    VARCHAR(64),
    g_table VARCHAR(64),
    g_index VARCHAR(64),
    g_col   VARCHAR(64)
)
BEGIN
    DECLARE IndexExists INTEGER;
    SELECT COUNT(1) INTO IndexExists
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_schema = g_db
    AND table_name = g_table
    AND index_name = g_index;
    
    IF IndexExists = 0 THEN
        SET @sqlstmt = CONCAT('CREATE INDEX ', g_index, ' ON ', g_db, '.', g_table, ' (', g_col, ')');
        PREPARE st FROM @sqlstmt;
        EXECUTE st;
        DEALLOCATE PREPARE st;
    ELSE
        SELECT CONCAT('Index ', g_index, ' exists.') CreateindexErrorMessage;
    END IF;
END""")

#Creates unique index needed for 'insert into ... on duplicate key update'
engine.execute("CALL CreateIndex('atlas_bls', 'fact', 'fact_UQ', \
    'SeriesID,date')")

#Then inserts incubator entries into fact table, updating data when possible
#Finally, drops the incubator table
engine.execute("INSERT INTO fact (`SeriesID`, `data`, `date`) SELECT"
    " i.`SeriesID`, i.`data`, i.`date` FROM incubator AS i ON DUPLICATE"
    " KEY UPDATE `data` = VALUES(`data`)")
engine.execute("TRUNCATE TABLE incubator")

print "to the moon!"
