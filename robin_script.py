#
from src.projects.dima.dima_handler import main_translate, table_collector
from src.projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.projects.tall_tables.talltables_handler import ingesterv2
single_mdb = r""
mdb_dir = r""
# checking tables for single mdb
table_collector(single_mdb)

# create a single dataframe for a single table from a single MDB
main_translate("tblBSNE_Box",single_mdb)

#create a single dataframe for a single table from ALL mdbs in a directory

looper(mdb_dir,"tblBSNE_Box", csv=False)
