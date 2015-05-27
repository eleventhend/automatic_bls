import time
import csv

import json
from urllib2 import Request, urlopen

from sqlalchemy import create_engine
import MySQLdb
import pandas as pd
from bls import get_series

def dim_maker(df, engine, output_table):
    pd.DataFrame.to_sql(df, con=engine, name=output_table, 
        if_exists='append', index=False)

    dd_statement = "CREATE TABLE dedupe as SELECT * FROM %(opt)s WHERE 1 "
    " GROUP BY %(col_zero)s, %(col_one)s;" % {"opt": output_table, "col_zero":\
     df.columns[0], "col_one": df.columns[1]}
    drop_statement = "DROP TABLE %(opt)s;" % {"opt": output_table}
    rename_statement = "RENAME TABLE dedupe TO %(opt)s;" % {"opt": output_table}

    engine.execute(dd_statement)
    engine.execute(drop_statement)
    engine.execute(rename_statement)
    return

with open ('sql_engine.txt', 'r') as f:
    engine_address = f.read()

engine = create_engine(engine_address)

print "Welcome to the Atlas Dimension Populator 3000"

pre_df = pd.read_csv("prefix.csv")
geo_df = pd.read_csv("BLS_County_Codes.csv")
LA_mc_df = pd.read_csv("LA/Measure_Codes.csv", 
    converters={'measure_code': lambda x: str(x)})

dim_maker(pre_df, engine, "dim_prefix")
dim_maker(geo_df, engine, "dim_geo")
dim_maker(LA_mc_df, engine, "dim_LA_mc")

print "to the moon!"
