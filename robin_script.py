#
from src.utils.tools import db
from src.projects.dima.dima_handler import main_translate, table_collector, looper, batch_looper
from src.projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.projects.tall_tables.talltables_handler import ingesterv2

from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tables.lpipk import lpi_pk
from src.projects.dima.tabletools import fix_fields
single_mdb = r"" # absolute path to single mdb
mdb_dir = r"" # absolute path to directory for a batch of mdb's
d = db("dima") # requires 'database.ini' file to have postgres credentals

# checking tables for single mdb

table_collector(mdb_dir)





arc = arcno()
# arc.MakeTableView("tblGapHeader",p3)

# create a single dataframe for a single table from a single MDB
head = main_translate('tblGapDetail',p3) # assigining dataframe to variable df

#create a single dataframe for a single table from ALL mdbs in a directory

gapdet = looper(mdb_dir,'tblGapDetail', csv=False)

gaphead = looper(mdb_dir,'tblGapHeader', csv=False)

 # assigning dataframe to variable df2

 # to check dataframe size
table_create(df,'tblGapDetail', "dima")
ingesterv2.main_ingest(df, "tblGapDetail", d.str, 100000) # sending dataframe to postgres
batch_looper(mdb_dir)










#
