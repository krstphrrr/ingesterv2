#
from src.utils.tools import db 
from src.projects.dima.dima_handler import main_translate, table_collector, looper, batch_looper
from src.projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.projects.tall_tables.talltables_handler import ingesterv2
single_mdb = r"" # absolute path to single mdb
mdb_dir = r"" # absolute path to directory for a batch of mdb's

d = db("dima") # requires 'database.ini' file to have postgres credentals

# checking tables for single mdb
table_collector(mdb_dir)

# create a single dataframe for a single table from a single MDB
df=main_translate("tblBSNE_Box",single_mdb) # assigining dataframe to variable df

#create a single dataframe for a single table from ALL mdbs in a directory

df2 = looper(mdb_dir,"tblBSNE_Box", csv=False) # assigning dataframe to variable df2
df.shape # to check dataframe size

ingesterv2.main_ingest(df, "tblHorizontalFlux", d.str, 10000) # sending dataframe to postgres
