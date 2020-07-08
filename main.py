from projects.tall_tables.talltables_handler import model_handler, field_parse, ingesterv2
from projects.tall_tables.models.gap import dataGap
from projects.dima.dima_handler import pg_send
from utils.arcnah import arcno


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
