import csv

from sqlalchemy import create_engine
import pandas as pd

def dim_add_dedupe(df, engine, output_table):

    pd.DataFrame.to_sql(df, con=engine, name=output_table, 
        if_exists='append', index=False)

    engine.execute("CREATE TABLE dedupe as SELECT DISTINCT * FROM %(opt)s;"\
     % {"opt": output_table})
    engine.execute("DROP TABLE %(opt)s;" % {"opt": output_table})
    engine.execute("RENAME TABLE dedupe TO %(opt)s;" % {"opt": output_table})
    return

print "Welcome to the Atlas Dimension Populator 3000"

with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()
engine = create_engine(engine_address)

prefix = pd.read_csv("prefix.csv")
dim_add_dedupe(prefix, engine, "dim_prefix")

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
        sector['db_prefix'] = p
        dim_add_dedupe(sector, engine, "dim_sector")
    except:
        sector = pd.DataFrame(index = [0], columns = ['sector_code'])
        sector = sector.fillna('')
    for x in range(0, len(seasonal.index), 1):
        for y in range(0, len(m_c.index), 1):
            for z in range(0, len(sector.index), 1):
                df_sql = pd.DataFrame(index = [0], columns = ['SeriesID', 
                    'db_prefix', 'seasonal_code', 'area_code', 'sector_code',
                    'measure_code'])
                for i in range(0, len(geo.index), 1):
                    ser_concat = (p + seasonal['seasonal_code'].iloc[x] + 
                        geo['area_code'].iloc[i] + 
                        sector['sector_code'].iloc[z] + 
                        m_c['measure_code'].iloc[y])
                    df_con = pd.DataFrame(index = [0], columns = ['SeriesID',
                        'db_prefix', 'seasonal_code', 'area_code', 'sector_code',
                        'measure_code'])
                    df_con['SeriesID'] = ser_concat
                    df_con['area_code'] = geo['area_code'].iloc[i]
                    df_sql = df_sql.append(df_con, True)
                df_sql['db_prefix'] = p 
                df_sql['sector_code'] = sector['sector_code'].iloc[z]
                df_sql['seasonal_code'] = seasonal['seasonal_code'].iloc[x]
                df_sql['measure_code'] = m_c['measure_code'].iloc[y]
                df_sql = df_sql.dropna()
                dim_add_dedupe(df_sql, engine, "dim_series")
    geo['db_prefix'] = p
    m_c['db_prefix'] = p
    seasonal['db_prefix'] = p
    dim_add_dedupe(geo, engine, "dim_geo")
    dim_add_dedupe(m_c, engine, "dim_measure_codes")
    dim_add_dedupe(seasonal, engine, "dim_seasonal")

print "to the moon!"
