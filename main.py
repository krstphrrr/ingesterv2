from projects.tall_tables.talltables_handler import model_handler, field_parse, ingesterv2
from projects.tall_tables.models.gap import dataGap

# path = r"C:\Users\kbonefont\Desktop\data\gap_tall.csv"

"""
**once projecttype is tall!:

1. instantiate model_handler with path, dictionary, tablename
inst = model_handler(path,dataGap,"dataGap")

2. create an df with its field checked (overriding pandas/csv's default parsing)
inst.checked()
inst.checked_df.shape

3. instantiate ingesterv2: tools for table drop, foreign key set/drop
i = ingesterv2()
i.drop_fk('dataGap')
i.drop_table('dataGap')

4. create empty table
inst.create_empty_table()

5. ingest
i.main_ingest(inst.checked_df,"dataGap",db.str,100000)
"""

def main():
    proj = None
    pth = None
    fld = None
    tbl = None
    while proj is None and pth is None and fld is None and tbl is None:
        proj = input('please input project(tall, nri, met, or dima): ')
        print('project set.')
        pth = input('please input path: ')
        print('path set.')
        fld = input('please input dictionary with fields: ')
        if 'dataGap' in fld:
            fld = dataGap
        print('field dictionary set.')
        tbl = input('please input table name: ')
        print('table name set.')
    else:
        a = request_handler(proj,pth,fld,tbl)
        print('ok')





class request_handler:
    tablename = None
    fields = None
    path = None

    modelhandler = None
    ingester = None

    projectswitch = None

    __projects = {
        'a':'tall',
        'b':'nri',
        'c':'met',
        'd':'dima'
        }

    def __init__(self, projecttype:str, path:str, dictionary:{}, tablename:str):
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.path = path
        self.fields = dictionary
        self.tablename = tablename
        self.projectswitch = self.__projects[projecttype]


    def clear(self,var):
        var = None
        return var

    def set_model(self):
        if self.projectswitch=='tall':
            self.modelhandler = model_handler(self.path, self.fields, self.tablename)
        else:
            print('handling not implemented')

    def set_ingest(self):
        if self.projectswitch=='tall':
            self.ingester = ingesterv2()
        else:
            print('handling not implemented')




if __name__=="__main__":
    main()
































#
