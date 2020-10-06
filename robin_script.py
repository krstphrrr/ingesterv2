#
from src.utils.tools import db
from src.projects.dima.dima_handler import main_translate, table_collector, looper, batch_looper
from src.projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.projects.tall_tables.talltables_handler import ingesterv2
single_mdb = r"" # absolute path to single mdb
mdb_dir = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs" # absolute path to directory for a batch of mdb's
from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tables.lpipk import lpi_pk
from src.projects.dima.tabletools import fix_fields

d = db("dima") # requires 'database.ini' file to have postgres credentals
p1 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\8May2017 DIMA 5.5a as of 2020-03-10.mdb"
p2 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\21May2015 DIMA 5.5a as of 2020-03-10.mdb"
p3 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\REPORT 5May15 - 5Mar19 JER DIMA 5.4 as of 2019-04-19.mdb"
p4 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\REPORT 7Jun19 JER DIMA 5.4 as of 2019-04-19.mdb"
p5 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\REPORT 13Dec19 JER DIMA 5.4 as of 2019-04-19.mdb"
p6 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\REPORT 18Sept19 JER DIMA 5.4 as of 2019-04-19.mdb"
p7 = r"C:\Users\kbonefont\Desktop\dimas\Network_DIMAs\REPORT 31Oct19 JER DIMA 5.4 as of 2019-04-19.mdb"
# checking tables for single mdb
table_collector(mdb_dir)





arc = arcno()
# arc.MakeTableView("tblGapHeader",p3)

# create a single dataframe for a single table from a single MDB
head = main_translate('tblGapDetail',p3) # assigining dataframe to variable df

#create a single dataframe for a single table from ALL mdbs in a directory

gapdet = looper(mdb_dir,'tblGapDetail', csv=False)

gaphead = looper(mdb_dir,'tblGapHeader', csv=False)
gaphead.columns
for i in gapdet.RecKey.unique():
    if i not in gaphead.RecKey.unique():
        print(i)
 # assigning dataframe to variable df2

 # to check dataframe size
table_create(df,'tblGapDetail', "dima")
ingesterv2.main_ingest(df, "tblGapDetail", d.str, 100000) # sending dataframe to postgres
batch_looper(mdb_dir)



project table updating on dima ingest

when formal resignation letter cc'ing sarah






#
