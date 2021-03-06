import os, os.path, sys
# print(os.getcwd())
# os.chdir("/usr/src")
from src.projects.tall_tables.talltables_handler import model_handler, field_parse, ingesterv2
from src.projects.tall_tables.models.gap import dataGap
from src.projects.dima.dima_handler import pg_send, batch_looper, has_duplicate_pks
from src.projects.project import update_project, all_dimas


from src.utils.arcnah import arcno

def main():
    proj = None
    pth = None
    fld = None
    tbl = None
    dimadict = {i[0]:i[1] for i in enumerate(os.listdir(os.path.join(os.getcwd(),"dimas"))) if '.mdb' in i[1]}

    while proj is None and pth is None and fld is None and tbl is None:
        proj = "dima"
        if "dima" in proj:
            batch_single = "b"
            if 'b' in batch_single:
                print('selected path: ')


            elif 's' in batch_single:
                print('selected single file dima')


    else:
        # a = request_handler(proj,pth,fld,tbl)
        contin=False
        while contin ==False:
            if 'dima' in proj and 'b' in batch_single and contin==False:
                batch_path = os.path.normpath(os.path.join(os.getcwd(),"dimas"))
                print(f"using batch path -> {batch_path}")
                tocsv = "pg"

                if tocsv=='pg':
                    # first update project key
                    dev_list = all_dimas("dimadev")
                    dima_list = all_dimas("dima")
                    print("list of dimas currently in the dev. DB : ")
                    print(dev_list)
                    print("list of dimas currently in the main dima DB: ")
                    print(dima_list)
                    print("Please enter 'ProjectKey' for ingestion batch: ")
                    projkey = sys.stdin.readline()
                    # projkey = "test_run"
                    # update_project(batch_path, projkey)
                    # then continue with batch processing
                    # print(f"your project key is: {projkey}")
                    if projkey is not None:
                        print("ingest to development database? true or false")
                        dev_or_not = sys.stdin.readline()
                        batch_looper(batch_path, projkey,dev=False, pg=True) if 'false' in dev_or_not else batch_looper(batch_path, projkey,dev=True, pg=True)
                        update_project(batch_path, projkey, "dimadev") if "true" in dev_or_not else update_project(batch_path, projkey, "dima")
                    else:
                        print("Please input a 'ProjectKey' for this ingestion")
                        continue
                    contin=True
                elif tocsv=='csv':
                    # first update project key
                    # projkey = input('set project key: ')
                    # update_project(batch_path, projkey)
                    # then continue with batch processing
                    batch_looper(batch_path, pg=False)
                else:
                    print('please select pg or csv')
                    continue

            elif 'dima' in proj and 's' in batch_single:
                print(f'select dima to ingest')
            # print('ok')

            elif 'aero' in proj:
                aero_dir = os.path.normpath(os.path.join(os.path.dirname(os.getcwd()),"aero"))
            break

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

    def __init__(self, projecttype:str, path:str):
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.path = path
        # self.fields = dictionary
        # self.tablename = tablename
        self.projectswitch = self.__projects[projecttype]


    def clear(self,var):
        var = None
        return var

    def set_model(self,fields=None, tablename=None, pg=None):
        if self.projectswitch=='tall':

            self.fields = fields
            self.tablename = tablename
            self.modelhandler = model_handler(self.path, self.fields, self.tablename)

        elif self.projectswitch=='dima':
            arc = arcno(self.path)
            for i,j in arc.actual_list.items():
                if 'Box' not in i:
                    pg_send(i,self.path) if pg==None else pg_send(i,self.path,1)


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
