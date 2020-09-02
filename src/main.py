import os, os.path
os.chdir(os.path.join(os.getcwd(),'src')) if os.path.basename(os.getcwd())!='src' else None

from projects.tall_tables.talltables_handler import model_handler, field_parse, ingesterv2
from projects.tall_tables.models.gap import dataGap
from projects.dima.dima_handler import pg_send, batch_looper, has_duplicate_pks
from projects.project import update_project


from utils.arcnah import arcno

def main():
    proj = None
    pth = None
    fld = None
    tbl = None
    dimadict = {i[0]:i[1] for i in  enumerate(os.listdir(os.path.join(os.path.dirname(os.getcwd()),"dimas"))) if '.mdb' in i[1]}

    while proj is None and pth is None and fld is None and tbl is None:
        proj = input('please input project(tall, nri, met, or dima): ')
        if "dima" in proj:
            batch_single = input('please select \'b\'(batch of dimas) or \'s\'(single dima): ')
            if 'b' in batch_single:
                print('selected batch single')
            elif 's' in batch_single:
                print('selected single file dima')

    else:
        # a = request_handler(proj,pth,fld,tbl)
        contin=False
        while contin ==False:
            if 'dima' in proj and 'b' in batch_single and contin==False:
                print('current directory to batch ingest: ')
                batch_path = os.path.normpath(os.path.join(os.path.dirname(os.getcwd()),"dimas"))

                cont = input('continue? y or n ')
                if cont=='y':
                    # first update project key
                    projkey = input('set project key: ')
                    update_project(batch_path, proj)
                    # then continue with batch processing
                    batch_looper(batch_path)

                    """
                    1.function to cycle through each table of each dima and
                    and check if any of tables has primarykeys in postgres
                    returns true if all tables have new primary keys
                    false if ANY of the tables have duplicate primary keys on the db.

                        - maybe should be a class so it stores an object property with a list of
                        duplicate pk's per table

                    2a. if no duplicate primary keys, ingest whole batch with batch_looper

                    2b. if any duplicate key, ask if ingestion of individual tables is prefered

                    """
                    contin=True
                elif cont=='n':
                    print('action aborted')
                    continue
                else:
                    print('please select y or n')

            elif 'dima' in proj and 's' in batch_single:
                print(f'select dima to ingest')
            print('ok')
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
