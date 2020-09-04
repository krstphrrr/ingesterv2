import os, os.path
import pandas as pd
path = r"C:\Users\kbonefont\Desktop\aero_flux_output"

txt_read(path)

def txt_read(path):
    df_dict = {}
    testset = ["20184145384203B2_flux","20184145384203B1_flux","20184145374203B2_flux"]
    count = 1
    for i in os.listdir(path):
        # debug block
        if os.path.splitext(i)[0] in [i for i in testset]:
            complete = os.path.join(path,i)
            temp = pd.read_table(complete, sep="\t", low_memory=False)
            df_dict.update({f"df{count}":temp})
            count+=1
        
        complete = os.path.join(path,i)
        temp = pd.read_table(complete, sep="\t", low_memory=False)
        df_dict.update({f"df{count}":temp})
        count+=1


    return pd.concat([d[1] for d in df_dict.items()],ignore_index=True)
