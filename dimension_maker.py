import csv

from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def add_dedupe(df, engine, table, uniques, id_name):
    """
    add_dedupe appends a given pandas dataframe to a given sql table: 
        table using a given sqlalchemy engine, using an intermediary
        table (dedupe) to ensure no duplicates are added.
    Inserts a column:
        id_name
    which is populated with nan to allow receiving sql table to populate 
        a unique primary key for each added dimension via auto_increment
    """
    df.insert(0, ('%(id_name)s' %{"id_name": id_name}), np.nan)

    df.to_sql(name="dedupe", con=engine, if_exists='replace', index=False)

    engine.execute("INSERT INTO %(table)s SELECT * FROM dedupe \
        WHERE (%(uniques)s) NOT IN (SELECT %(uniques)s FROM %(table)s)" 
        %{"table": table, "uniques": uniques})

    return

#import and setup SQL engine address
with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()
engine = create_engine(engine_address)

#creates series, measure, area, and sector tables
engine.execute("CREATE TABLE IF NOT EXISTS dim_series (`series_id` \
    int(11) NOT NULL AUTO_INCREMENT, `series_code` varchar(64), `prefix` \
    varchar(4), `prefix_long` varchar(6), `prefix_desc` text, \
    `seasonal_code` varchar(2), `seasonal_desc` text, \
    `area_code` varchar(32), `sector_code` varchar(16), \
    `sector_text` varchar(128), `measure_code` varchar(4), \
    `measure_text` varchar(128), PRIMARY KEY (`series_id`))")
engine.execute("CREATE TABLE IF NOT EXISTS dim_measure \
    (`measure_id` int(11) NOT NULL AUTO_INCREMENT, `measure_code` varchar(4), \
    `measure_text` text, `prefix` varchar(4), PRIMARY KEY (measure_id))")
engine.execute("CREATE TABLE IF NOT EXISTS dim_area (`area_id` int(11) \
    NOT NULL AUTO_INCREMENT, `area_type_code` int(2), `area_code` \
    varchar(32), `area_text` varchar(64), `state` varchar(2), `state_id` \
    varchar(2), `area_fips_id` varchar(10), `prefix` varchar(2), \
    PRIMARY KEY (`area_id`))")
engine.execute("CREATE TABLE IF NOT EXISTS dim_sector (`sector_id` \
    int(11) NOT NULL AUTO_INCREMENT, `sector_code` varchar(16), \
    `sector_text` varchar(128), `prefix` varchar(4), PRIMARY KEY \
    (`sector_id`))")

#gets all dataset prefixes as listed in prefix.csv
prefix = pd.read_csv("prefix.csv")

#initialize empty dataframe
df_empty = pd.DataFrame(index=[0], columns=['series_code', 
    'prefix', 'prefix_long', 'prefix_desc', 'seasonal_code', 'seasonal_desc', 
    'area_code', 'sector_code', 'sector_text', 'measure_code', 'measure_text'])

"""
Fills the empty dataframe with:
    concatenated series codes (reason for nested for loops)
    individual series components
Periodically uploads the data to the sql database, in order to prevent 
    stack overflow from occuring
"""
for aa in range(0, len(prefix.index), 1):
    p = prefix['prefix'].iloc[aa]
    area = pd.read_csv(p + "/area_codes.csv",
        converters={
            'area_code': lambda x: str(x), 
            'state_id': lambda y: str(y), 
            'area_fips_id': lambda z: str(z)
        })
    m_c = pd.read_csv(p + "/measure_codes.csv",
        converters={'measure_code': lambda x: str(x)})
    seasonal = pd.read_csv(p + "/seasonal_codes.csv")
    try:
        sector = pd.read_csv(p + "/sector_codes.csv",
            converters={'sector_code': lambda x: str(x)})
        sector['prefix'] = p
        add_dedupe(sector, engine, "dim_sector", "sector_code, sector_text", "sector_id")
    except:
        sector_none = pd.DataFrame(index=[0], columns=['sector_code', 
            'sector_text', 'prefix'])
        sector_none['sector_code'] = "none"
        sector_none['sector_text'] = "default no sector"
        #sector_none['prefix'] = p
        add_dedupe(sector_none, engine, "dim_sector", "sector_code, sector_text", "sector_id")
        sector = pd.DataFrame(index=[0], columns=['sector_code'])
        sector = sector.fillna('')
    for x in range(0, len(seasonal.index), 1):
        for y in range(0, len(m_c.index), 1):
            for z in range(0, len(sector.index), 1):
                df_series = df_empty
                for i in range(0, len(area.index), 1):
                    ser_concat = (p + seasonal['seasonal_code'].iloc[x] + 
                        area['area_code'].iloc[i] + 
                        sector['sector_code'].iloc[z] + 
                        m_c['measure_code'].iloc[y])
                    df_concat = df_empty
                    df_concat['series_code'] = ser_concat
                    df_concat['area_code'] = area['area_code'].iloc[i]
                    df_series = df_series.append(df_concat, True)
                df_series['prefix'] = p 
                df_series['prefix_long'] = prefix['prefix_long'].iloc[aa]
                df_series['prefix_desc'] = prefix['prefix_desc'].iloc[aa]
                if sector['sector_code'].iloc[z] == '':
                    df_series['sector_code'] = 'none'
                    df_series['sector_text'] = 'default no sector'
                else:
                    df_series['sector_code'] = sector['sector_code'].iloc[z]
                    df_series['sector_text'] = sector['sector_text'].iloc[z]
                df_series['seasonal_code'] = seasonal['seasonal_code'].iloc[x]
                df_series['seasonal_desc'] = seasonal['seasonal_text'].iloc[x]
                df_series['measure_code'] = m_c['measure_code'].iloc[y]
                df_series['measure_text'] = m_c['measure_text'].iloc[y]
                add_dedupe(df_series, engine, "dim_series", "series_code", "series_id")
    area['prefix'] = p
    m_c['prefix'] = p
    seasonal['prefix'] = p
    add_dedupe(area, engine, "dim_area", "area_code, area_text", "area_id")
    add_dedupe(m_c, engine, "dim_measure", "measure_code, measure_text", "measure_id")
