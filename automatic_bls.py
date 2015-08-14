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

def api_to_sql(engine, series, api_key, start_year, end_year):
    """
    Calls the API using get_series(), returning a dataframe version of the JSON
        response from the API

    Outputs to a sql database a fact table based on an input of
        a tuple of tuples of series to fetch and a desired date range.

    **TODO: Improve error handling by:
        Determining exactly which codes are breaking
        Retrying broken 50 code series (minus broken codes)
    """
    try:
        df = get_series(series, api_key, start_year, end_year)
        for k in range (1, len(df.columns)):
            df_sql = dataframe_sequencer(df, k)
            df_sql.to_sql(con=engine, name='incubator', if_exists='append', 
                index=False)
        time.sleep(5)
    except Exception as e:
        with open('failed_series.txt', 'a') as fs_file:
            fs_file.write('\n'.join(series))
            fs_file.write('\n')
        with open('errors.txt', 'a') as errorfile:
            errorfile.write("Error: %s \n" % (e,))
            errorfile.write('\n'.join(series))
            errorfile.write('\n' + api_key)
            errorfile.write('\n' + start_year)
            errorfile.write('\n' + end_year + '\n')
        with open ('type_errors.txt', 'a') as te_file:
            te_file.write("Error: %s \n" % (e,))
        time.sleep(5)
        return
    return

def dataframe_sequencer(df, i):
    """
    Takes a dataframe of raw api data and transforms it into the format
        required by the fact table (3 columns, series/data/date)
    """
    df_sql = pd.DataFrame({'date':[], 'series':[], 'data':[]})
    df_sql['date'] = df.index
    df_sql.index = df.index
    df_sql['data'] = df[df.columns[i]]
    df_sql['series'] = df.columns[i]
    return df_sql

def data_extractor(engine, prefix, seasonal, area, m_c, api_key, startyear, 
    endyear, sector=pd.DataFrame()):
    """
    Uses previously defined functions to create batches of series, sends 
        them to the BLS api, and inserts the returned data to an incubator
        table in the SQL server.
    Nested FOR loops are used to create all possible series code combinations
    """
    try:
        sector['sector_code'].iloc[0]
    except:
       sector = pd.DataFrame(index=[0], columns=['sector_code'])
       sector = sector.fillna('')
    allseries = ()
    for x in range(0, len(seasonal.index), 1):
        for y in range(0, len(m_c.index), 1):
            for z in range(0, len(sector.index), 1):
                for i in range(0, len(area.index), 1):
                    ser_concat = (prefix + seasonal['seasonal_code'].iloc[x] + 
                        area['area_code'].iloc[i] + 
                        sector['sector_code'].iloc[z] + 
                        m_c['measure_code'].iloc[y])
                    allseries = allseries + (ser_concat,)
    series = [allseries[i:i+49] for i in range(0, len(allseries), 50)]
    for i in range(0, len(series)):
        api_to_sql(engine, series[i], api_key, startyear, endyear)
    return

def create_index(engine, table, index, column):
    """
    Checks whether a specific index exists on a column in a table;
        if it does not, it is created
    """
    result = engine.execute(
        """SELECT COUNT(1) FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = %(t)s AND index_name = %(i)s;""" 
        %{"t": "'" + table + "'", "i": "'" + index + "'"})

    index_exists = result.fetchall()
    if index_exists[0][0] == 0:
        engine.execute("CREATE INDEX %(i)s ON %(t)s(%(c)s)"
            %{"t": table, "i": index, "c": column})
    return

def cross_populate(engine, fact, dim, f_join, d_join, f_id="", d_id="", 
    dimx="", dx_join = "", dx_id = "", series_run=False):
    """
    Updates an id column in the given fact-style table with the ids from a 
        given dimension table.
    There is a special condition for running this function on the series 
        dimension table (or date table), since all other cross-populations 
        require a join on the series dimension table as well as the target 
        dimension table.
    """
    if series_run == True:
        create_index(engine, fact, f_join, f_join)
        create_index(engine, dim, d_join, d_join)
        engine.execute("""UPDATE %(f)s f JOIN %(d)s dts ON \
            f.%(fj)s = dts.%(dj)s SET f.%(fi)s = dts.%(di)s""" 
            %{"f": fact, "d": dim, "fj": f_join, "dj": d_join, "fi": f_id, 
            "di": d_id})
    else:
        create_index(engine, dim, d_join, d_join)
        create_index(engine, dimx, dx_join, dx_join)
        engine.execute("""UPDATE %(f)s f JOIN %(d)s dts ON \
            f.%(fj)s = dts.%(dj)s JOIN %(dx)s dtx ON \
            dts.%(dxj)s = dtx.%(dxj)s AND dts.`prefix` = dtx.`prefix` \
            SET f.%(dxi)s = dtx.%(dxi)s""" 
            %{"f": fact, "d": dim, "fj": f_join, "dj": d_join, 
            "dx": dimx, "dxj": dx_join, "dxi": dx_id})
    return

#Creates and retrieves command line arguments
#which determine the target dataset and year range
parser = argparse.ArgumentParser(description='Retrieve BLS data', 
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("Prefix", help="Requires the prefix of the desired \
    database as an argument")
parser.add_argument("--start_year", type=int, default=(date.today().year-1), 
    choices=xrange(1980, (date.today().year)), 
    metavar='1980-%(y)i' %{"y": (date.today().year-1)},
    help='Optional - start year for data range.')
parser.add_argument("--end_year", type=int, default=(date.today().year), 
    choices=xrange(1980, (date.today().year+1)), 
    metavar='1980-%(y)i' %{"y": date.today().year},
    help='Optional - end year for data range. Year range cannot exceed 10.')
args = parser.parse_args()
prefix = args.Prefix
startyear = args.start_year
endyear = args.end_year
#Ensures year range is confined to 10 years; converts inputs to strings
if (endyear-startyear>10):
    endyear = startyear+10
startyear = str(startyear)
endyear = str(endyear)

#Get API key and SQL engine address (secure info)
with open ('api_key.txt', 'r') as f:
    api_key = f.read()
with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()

#set up sqlalchemy engine
engine = create_engine(engine_address)

#Get series code components from CSV
s_df = pd.read_csv(prefix + "/seasonal_codes.csv")
area_df = pd.read_csv(prefix + "/area_codes.csv",
    converters={'area_code': lambda x: str(x)})
mc_df = pd.read_csv(prefix + "/measure_codes.csv",
    converters={'measure_code': lambda x: str(x)})

#set up empty incubator table
engine.execute("CREATE TABLE IF NOT EXISTS incubator (`incubator_id` int(11) \
    NOT NULL AUTO_INCREMENT, `series` varchar(64), `data` float, `date` \
    datetime, `series_id` int(11), `area_id` int(11), `sector_id` int(11), \
    `measure_id` int(11), `date_id` int(11), `retrieval_date` TIMESTAMP \
    DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (incubator_id))")
engine.execute("TRUNCATE TABLE incubator")

"""
If there is a sector file for this database, it is read in and used
If there is no sector file, no sector input is used
Extraction occurs here--creates and splits the series codes into batches
   of 50 series codes and sends them to the API. The returned data is inserted
   into the incubator table.
"""
try:
    sector_df = pd.read_csv(prefix + "/sector_codes.csv",
        converters={'sector_code': lambda x: str(x)})
    data_extractor(engine, prefix, s_df, area_df, mc_df, api_key, 
        startyear, endyear, sector_df)
except:
    data_extractor(engine, prefix, s_df, area_df, mc_df, api_key, 
        startyear, endyear)

#Checks that fact and archive tables exist
engine.execute("CREATE TABLE IF NOT EXISTS fact (`fact_id` int(11) NOT NULL \
    AUTO_INCREMENT, `series` varchar(64), `data` float, `date` datetime, \
    `series_id` int(11), `area_id` int(11), `sector_id` int(11), \
    `measure_id` int(11), `date_id` int(11), `retrieval_date` TIMESTAMP \
    DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (fact_id), \
    CONSTRAINT fact_UQ UNIQUE (series_id, date_id))")
engine.execute("CREATE TABLE IF NOT EXISTS archive (`archive_id` int(11) \
    NOT NULL AUTO_INCREMENT, `series` varchar(64), `data` float, `date` \
    datetime, `series_id` int(11), `area_id` int(11), `sector_id` int(11), \
    `measure_id` int(11), `date_id` int(11), `retrieval_date` TIMESTAMP, \
    `archivedate` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
    PRIMARY KEY (archive_id))")
#Inserts changed fact entries into the archive table
engine.execute("INSERT INTO archive (`series`, `data`, `date`, `series_id`, \
    `area_id`, `sector_id`, `measure_id`, `date_id`, `retrieval_date`) SELECT \
    f.`series`, f.`data`, f.`date`, f.`series_id`, f.`area_id`, \
    f.`sector_id`, f.`measure_id`, f.`date_id`, f.`retrieval_date` \
    FROM `fact` AS f JOIN `incubator` AS i ON f.`series` = i.`series` \
    AND f.`date` = i.`date` AND f.`data` != i.`data`")

#Checks that date table exists; inserts date data into date dimension table
engine.execute("CREATE TABLE IF NOT EXISTS dim_date \
    (`date_id` int(11) NOT NULL AUTO_INCREMENT, `date_full` TIMESTAMP, \
    `year` int(4), `month` int(2), `month_name` varchar(10), \
    PRIMARY KEY (`date_id`))")
engine.execute("INSERT INTO dim_date \
    (`date_full`, `year`, `month`, `month_name`) \
    SELECT i.`date`, YEAR(i.`date`), MONTH(i.`date`), MONTHNAME(i.`date`) \
    FROM incubator AS i WHERE i.`date` NOT IN \
    (SELECT `date_full` FROM dim_date) GROUP BY i.`date`")

#cross-populates foreign keys to fact table from all dimension tables
cross_populate(engine, "incubator", "dim_series", "series", "series_code", 
    f_id="series_id", d_id="series_id", series_run=True)
cross_populate(engine, "incubator", "dim_series", "series_id", "series_id", 
    dimx="dim_area", dx_join="area_code", dx_id="area_id")
cross_populate(engine, "incubator", "dim_series", "series_id", "series_id", 
    dimx="dim_measure", dx_join="measure_code", dx_id="measure_id")
cross_populate(engine, "incubator", "dim_series", "series_id", "series_id", 
    dimx="dim_sector", dx_join="sector_code", dx_id="sector_id")
#wizardy here: the use case for the date table is the same as the series table
cross_populate(engine, "incubator", "dim_date", "date", "date_full", 
    f_id="date_id", d_id="date_id", series_run=True)

#Inserts incubator entries into fact table, updating data when possible
engine.execute("INSERT INTO fact (`series`, `data`, `date`, `series_id`, \
    `area_id`, `sector_id`, `measure_id`, `date_id`) SELECT \
     i.`series`, i.`data`, i.`date`, i.`series_id`, i.`area_id`, \
     i.`sector_id`, i.`measure_id`, i.`date_id` FROM incubator AS i \
     ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)")

#creates indexes in fact table where needed
create_index(engine, "fact", "series_id", "series_id")
create_index(engine, "fact", "area_id", "area_id")
create_index(engine, "fact", "sector_id", "sector_id")
create_index(engine, "fact", "measure_id", "measure_id")
create_index(engine, "fact", "date_id", "date_id")
