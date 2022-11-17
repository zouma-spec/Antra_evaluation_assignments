#question 1
import pandas as pd

df1=pd.read_csv('./People/people_1.txt', sep='\t')
df2=pd.read_csv('./People/people_2.txt', sep='\t')
df = pd.concat([df1, df2],ignore_index=True)

df['FirstName'] = df['FirstName'].str.lower().str.strip()
df['LastName'] = df['LastName'].str.lower().str.strip()
df['Email'] = df.Email.str.lower().str.strip()
df['Phone'] = df.Phone.str.replace('-','').str.strip()
df['Address']=df['Address'].str.replace('#','').str.replace('No.','').str.strip().str.lower()

df.drop_duplicates(inplace=True)
df.to_csv('./People/people_cleaned.csv',index=False)


#question 2
import json
import math

with open('movie.json', 'r', encoding='utf-8') as f1:
    js = json.load(f1)
    
    #specify the number of splits
    chucks=8
    js=js['movie']
    print(len(js))
    records_split=math.ceil((len(js)/chucks)) #rounding up numbers avoiding data loss
    
    for i in range(chucks):
        json.dump(js[i * records_split:(i+1) * records_split], 
                  open("./MovieFiles/movie_split_" + str(i+1) + ".json", 'w', encoding='utf8'), indent=True)
