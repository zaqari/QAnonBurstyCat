import pandas as pd
import numpy as np

# If on the remote server
from kgen2.LM.LM.BERT.BERT import *
from kgen2.LM.LM.BERT.data_loader import *

PATH = '/home/zaq/d/QAnon/'
dataset = 'qcult-exp.csv'
output_name = 'qcvecs.csv'

# For Trump data . . .
# df = pd.read_csv(PATH + dataset)
# df = df.loc[df['w'].isin(['trump'])].copy()
# df.index=range(len(df))
# df = df.loc[df['year'].isin([2020, '2020'])]

# Otherwise . . .
df = pd.read_csv(PATH + dataset)

print('pre-dropping of duplicates', len(df))
df = df.drop_duplicates()
print(list(df))
# df = df.drop_duplicates(subset=['day', 'month', 'year', 'text'])
# df.index=range(len(df))

print(list(df))
print(PATH, len(df))

meta_data = ['uid', 'uname', 'favorites', 'retweets', 'minute', 'hour', 'day', 'month', 'year', 'w', 'text']
level = [8,-1]
mod = BERT(device='cuda', special_tokens=True)
loader = data(PATH+dataset, mod.tokenizer, add_special_tokens=True)
loader.df = None

#(1) Set up .csv file for data repo
data = pd.DataFrame(columns=meta_data+['vec'])
data.to_csv(PATH+output_name, index=False, encoding='utf-8')

#(2) Generate embeddings with appropriate metadata
for k in df.index:
    w, text = df[['w', 'text']].loc[k]
    # print(text, '\n')
    try:
        vecs, _ = mod(str(text).split('http')[0].lower(), level=level)
        
        delta = loader.delta(w.lower(), str(text).lower().split('http')[0])
        # print(vecs.shape,delta)
        # print(w, text)
        # print()
        update = [df[meta_data].loc[k].tolist() + [str(vec.view(-1).cpu().tolist())] for vec in vecs[delta]]
        update = pd.DataFrame(np.array(update), columns=list(data))
        update.to_csv(PATH+output_name, index=False, encoding='utf-8', header=False, mode='a')

    except ValueError:
        0
    except IndexError:
        0
    except RuntimeError:
        0
