import pandas as pd
import numpy as np

kw = 'qcult'
folder_path = '/home/zaq/d/QAnon/'
output_path = folder_path + kw + '-exp.csv'

meta_data_cols = ['uid', 'uname', 'favorites', 'retweets', 'minute', 'hour', 'day', 'month', 'year', 'text', 'hashtags']

df = pd.DataFrame(columns=meta_data_cols+['w'])
df.to_csv(output_path, index=False, encoding='utf-8')

def tap(x):
    return x

def remove_chaffe(x):
    X = str(x).replace('\n', ' ')
    while '  ' in X:
        X = X.replace('  ', ' ')
    return X

def save_to_persist(df, path=output_path):
    df.to_csv(path, index=False, header=False, encoding='utf-8', mode='a')

def text_handler(path, searchterms, chunksize=2000, process=save_to_persist, start_at=0):

    Searchterms = [i.lower() for i in searchterms]

    ct = 0
    for chunk in pd.read_csv(path,
                             chunksize=chunksize,
                             names=list(df),
                             lineterminator='\n'
                             # nrows=100
                             ):
        if ct >= start_at:
            sel = np.array([(sum([term in text.lower() for term in Searchterms]) > 0) for text in chunk['text'].values])
            print('{}/{} statuses containing {} found'.format(ct, sel.sum(), searchterms))
            data_points = chunk.loc[sel]
            process(data_points)

            ct+=1
            log = np.array([ct]).reshape(-1,1)
            log=pd.DataFrame(log, columns=['epoch'])
            log.to_csv(folder_path+kw+'-log.csv', index=False, encoding='utf-8')


def add_w(input_path, output_path, searchterms, chunksize=2000, start_at=0):
    SEARCHTERMS = [w.lower() for w in searchterms]

    ct = 0
    for chunk in pd.read_csv(input_path,
                             chunksize=chunksize,
                             names=list(df),
                             lineterminator='\n'
                             # nrows=100
                             ):
        chunk.text = chunk.text.astype(str)
        if ct >= start_at:
            new_df = []
            for i in chunk.index:
                for w in SEARCHTERMS:
                    if w in chunk['text'].loc[i].lower():
                        new_df.append(chunk[meta_data_cols].loc[i].tolist() + [w])

            new_df = np.array(new_df)
            if len(new_df) > 0:
                new_df = pd.DataFrame(new_df, columns=meta_data_cols + ['w'])
                new_df['text'] = [remove_chaffe(text) for text in new_df['text'].values]
                new_df.to_csv(output_path, index=False, header=False, encoding='utf-8', mode='a')
                print('{}: {} statuses containing {} found'.format(ct, len(new_df), searchterms))
            ct+=1

def hashtag_handler(path, hashtags, chunksize=2000, process=save_to_persist, start_at=0):

    ct = 0
    for chunk in pd.read_csv(path,
                             chunksize=chunksize,
                             # nrows=100
                             ):
        if ct >= start_at:
            sel = np.array([(sum([term in text for term in hashtags]) > 0) for text in chunk['hashtags'].values])
            print('{}/{} statuses containing {} found'.format(ct, sel.sum(), hashtags))
            data_points = chunk.loc[sel]
            process(data_points)

            ct+=1
            log = np.array([ct]).reshape(-1,1)
            log=pd.DataFrame(log, columns=['epoch'])
            log.to_csv(folder_path+kw+'-log.csv', index=False, encoding='utf-8')

# text_handler(folder_path+'any.csv', ['covid', 'chinesevirus', 'chinavirus', 'china virus', 'cov-2', 'wuhan', 'vaccine', 'fauci'])
add_w(folder_path+'any.csv', output_path, ['covid', 'chinesevirus', 'chinavirus', 'china virus', 'cov-2', 'wuhan', 'vaccine', 'fauci', 'chinese virus', 'pedophile', 'adrenochrome'])
