import csv

from sqlalchemy import create_engine
import pandas as pd

def dim_maker(df, engine, output_table):

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

pre_df = pd.read_csv("prefix.csv")
dim_maker(pre_df, engine, "dim_prefix")

for x in range(0, len(pre_df.index), 1):
    p = pre_df['prefix'].iloc[x]
    geo_df = pd.read_csv(p + "/geo_codes.csv",
        converters={'area_code': lambda x: str(x)})
    mc_df = pd.read_csv(p + "/measure_codes.csv",
        converters={'measure_code': lambda x: str(x)})
    seasonal_df = pd.read_csv(p + "/seasonal_codes.csv")
    geo_df['db_prefix'] = p
    mc_df['db_prefix'] = p
    seasonal_df['db_prefix'] = p
    try:
        sector_df = pd.read_csv(p + "/sector_codes.csv",
            converters={'sector_code': lambda x: str(x)})
        sector_df['db_prefix'] = p
        dim_maker(sector_df, engine, "sector_df")
    except:
        pass
    dim_maker(geo_df, engine, "dim_geo")
    dim_maker(mc_df, engine, "dim_measure_codes")
    dim_maker(seasonal_df, engine, "dim_seasonal")

print "to the moon!"
