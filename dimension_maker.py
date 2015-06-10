import csv

from sqlalchemy import create_engine
import pandas as pd

def add_dedupe(df, engine, output_table):

    pd.DataFrame.to_sql(df, name=output_table, con=engine, 
        if_exists='append', index=False)

    engine.execute("CREATE TABLE dedupe as SELECT DISTINCT * FROM %(opt)s;"
        % {"opt": output_table})
    engine.execute("DROP TABLE %(opt)s;" % {"opt": output_table})
    engine.execute("RENAME TABLE dedupe TO %(opt)s;" % {"opt": output_table})
    return

with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()
engine = create_engine(engine_address)

engine.execute("CREATE TABLE IF NOT EXISTS dimtest_series (`id` int(11) NOT NULL \
    AUTO_INCREMENT, `series` varchar(64), `prefix` varchar(2), \
    `prefix_long` varchar(6), `seasonal_code` enum('S','U'), \
    `seasonal_desc` text, `area_code` varchar(32), `area_code_id` int, \
    `sector_code` varchar(16), `sector_code_id` int, \
    `measure_code` varchar(4), `measure_code_id` int, \
    PRIMARY KEY (`id`))")

engine.execute("CREATE TABLE IF NOT EXISTS dimtest_measure_codes (`id` int \
    NOT NULL AUTO_INCREMENT, `measure_code` varchar(2), `measure_text` text, \
    `prefix` varchar(2), PRIMARY KEY (id))")

engine.execute("CREATE TABLE IF NOT EXISTS dimtest_geo (`id` int NOT NULL \
    AUTO_INCREMENT, `area_type_code` varchar(1), `area_code` varchar(32), \
    `area_text` varchar(64), `state` varchar(2), `prefix` \
    varchar(2), PRIMARY KEY (`id`))")

"""sector needs to have a none row"""

engine.execute("CREATE TABLE IF NOT EXISTS dimtest_sector (`id` int NOT NULL \
    AUTO_INCREMENT, `sector_code` varchar(16), `sector_name` varchar(128), \
    `prefix` varchar(2), PRIMARY KEY (`id`))")

prefix = pd.read_csv("prefix.csv")

df_empty = pd.DataFrame(index = [0], columns = ['series', 
    'prefix', 'prefix_long', 'seasonal_code', 'seasonal_desc', 
    'area_code', 'sector_code', 
    'measure_code', 'measure_code'])

for x in range(0, len(prefix.index), 1):
    p = prefix['prefix'].iloc[x]
    geo = pd.read_csv(p + "/geo_codes.csv",
        converters={'area_code': lambda x: str(x)})
    m_c = pd.read_csv(p + "/measure_codes.csv",
        converters={'measure_code': lambda x: str(x)})
    seasonal = pd.read_csv(p + "/seasonal_codes.csv")
    try:
        sector = pd.read_csv(p + "/sector_codes.csv",
            converters={'sector_code': lambda x: str(x)})
        sector['prefix'] = p
        add_dedupe(sector, engine, "dimtest_sector")
    except:
        sector_none = pd.DataFrame(index = [0], columns = ['sector_code' 
            'sector_name', 'prefix'])
        sector_none['sector_code'] = "none"
        sector_none['sector_name'] = "default no sector"
        sector_none['prefix'] = p
        #add_dedupe(sector_none, engine, "dimtest_sector")
        sector = pd.DataFrame(index = [0], columns = ['sector_code'])
        sector = sector.fillna('')
    for x in range(0, len(seasonal.index), 1):
        for y in range(0, len(m_c.index), 1):
            for z in range(0, len(sector.index), 1):
                df_sql = df_empty
                for i in range(0, len(geo.index), 1):
                    ser_concat = (p + seasonal['seasonal_code'].iloc[x] + 
                        geo['area_code'].iloc[i] + 
                        sector['sector_code'].iloc[z] + 
                        m_c['measure_code'].iloc[y])
                    df_con = df_empty
                    df_con['series'] = ser_concat
                    df_con['area_code'] = geo['area_code'].iloc[i]
                    df_sql = df_sql.append(df_con, True)
                df_sql['prefix'] = p 
                if sector['sector_code'].iloc[z] == '':
                    df_sql['sector_code'] = 'none'
                else:
                    df_sql['sector_code'] = sector['sector_code'].iloc[z]
                df_sql['seasonal_code'] = seasonal['seasonal_code'].iloc[x]
                df_sql['measure_code'] = m_c['measure_code'].iloc[y]
                df_sql = df_sql.dropna()
                add_dedupe(df_sql, engine, "dimtest_series")
    geo['prefix'] = p
    m_c['prefix'] = p
    seasonal['prefix'] = p
    add_dedupe(geo, engine, "dimtest_geo")
    add_dedupe(m_c, engine, "dimtest_measure_codes")
    add_dedupe(seasonal, engine, "dimtest_seasonal")
