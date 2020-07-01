
import os
import pandas as pd

class header_fetch:

    def __init__(self, targetdir):
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.dir = targetdir
        self.all = [i for i in os.listdir(targetdir)]
        self.files = [i for i in os.listdir(targetdir) if i.endswith('.xlsx')==True]
        self.dirs = [i for i in os.listdir(targetdir) if os.path.splitext(i)[1]=='']

    def clear(self,var):
        var = None
        return var

    def pull(self,file, col=None):
        self.fields = []

        if (file in self.files) and (file.find('Coordinates')!=-1):
            temphead = pd.read_excel(os.path.join(self.dir,file))
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file in self.files) and ('Coordinates' not in file):
            full = pd.read_excel(os.path.join(self.dir,file))
            is_concern = full['Table name']==f'{col}'
            temphead = full[is_concern]
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file.endswith('.csv')) and ('Coordinates' not in file):
            full = pd.read_csv(os.path.join(self.dir,file))
            is_target = full['TABLE.NAME']==f'{col}'
            temphead = full[is_target]
            for i in temphead['FIELD.NAME']:
                self.fields.append(i)

        else:
            print('file is not in supplied directory')

# and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True) and (steps==0)



def extract_fields(path : str, which_dataset : str, tablelist:list):
    return_dict = {}
    steps = 0
    while steps<2:
        # print(f'step 1. steps value = {steps}')
        for file in os.listdir(path):
            # print(f'step 2. steps value = {steps}')
            if 'RangeChange2004-2008' in which_dataset:
                # print(f'step 3. steps value = {steps}')
                if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True) and (steps==0):
                    # print('extracting coordinates',steps)
                    header = header_fetch(path)
                    header.pull(file)
                    return_dict.update({'pointcoordinates':header.fields})
                    steps+=1

                if (file.find('2004')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True) and (steps==1):
                    for table in tablelist:
                        if 'POINTCOORDINATES' not in table:
                            # print('extracting the rest')
                            header = header_fetch(path)
                            header.pull(file, table)
                            return_dict.update({f'{table.lower()}':header.fields})
                            steps+=1

            else:
                print('which_dataset variable not implemented')
    else:
        # print('returning whole dict')
        return return_dict
