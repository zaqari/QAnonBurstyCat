import tweepy as tw
import pandas as pd
import numpy as np

api_key = 'U2hRtGDTDtQZX3vS613HmwMdc'
api_secret = 'pJ1Vo4ziiSDEml5ipbZsBGMhA0AqmzHO4qNENnN3rg8F1pzlLQ'
zro_access_token = '857095832910352384-edWVKvVYLA7pRI8I0EyLYv9L9UlscuI'
zro_access_secret = 'Aff6bNG8pxMYpd1to5qeeWXTZpuM8aIu4gvXQPnbTQzDd'

auth = tw.OAuthHandler(api_key, api_secret)
auth.set_access_token(zro_access_token, zro_access_secret)
api = tw.API(auth, wait_on_rate_limit=True)

NEW_COLLECTION = False

kw = 'any'
folder_path = '/home/zaq/d/QAnon/'
output_path = folder_path + kw + '.csv'

df = pd.DataFrame(columns=['uid', 'uname', 'favorites', 'retweets', 'minute', 'hour', 'day', 'month', 'year', 'text', 'hashtags'])
ckpt = pd.DataFrame(np.array([[-1]]),columns=['epoch'])
if NEW_COLLECTION:
    df.to_csv(output_path, index=False, encoding='utf-8')
    ckpt.to_csv(folder_path + kw + '-log.csv', index=False, encoding='utf-8')


def tap(x):
    return x

def save_to_persist(x, path=output_path):
    hashtags = [h['text'] for h in x.entities['hashtags']]
    output = [x.user.id, x.user.screen_name, x.favorite_count, x.retweet_count, x.created_at.minute, x.created_at.hour, x.created_at.day, x.created_at.month, x.created_at.year, x.text, ' '.join(hashtags)]
    d = pd.DataFrame(np.array(output).reshape(1,-1), columns=list(df))
    d.to_csv(path, index=False, header=False, encoding='utf-8', mode='a')

def text_handler(path, searchterms, chunksize=100, process=save_to_persist, start_at=-1):

    Searchterms = [i.lower() for i in searchterms]

    ct = 0
    for chunk in pd.read_csv(path,
                             names=['tweet_id'],
                             chunksize=chunksize,
                             # nrows=100
                             ):
        if ct > start_at:
            idlist = [i.split('_')[-1] for i in chunk['tweet_id'].values]
            statuses = api.lookup_statuses(id=idlist)
            tot = len(statuses)
            statuses = [status for status in statuses if sum([searchterm in status.text.lower() for searchterm in Searchterms]) > 0]
            print('{}/{} statuses containing {} found'.format(len(statuses), tot, searchterms))
            for status in statuses:
                process(status)
        ct+=1
        if ct >= start_at:
            log = np.array([ct]).reshape(-1,1)
            log=pd.DataFrame(log, columns=['epoch'])
            log.to_csv(folder_path+kw+'-log.csv', index=False, encoding='utf-8')


def hydrator(path, chunksize=100, process=save_to_persist, start_at=-1):

    ct = 0
    for chunk in pd.read_csv(path,
                             names=['tweet_id'],
                             chunksize=chunksize,
                             # nrows=100
                             ):
        if ct > start_at:
            idlist = [i.split('_')[-1] for i in chunk['tweet_id'].values]
            statuses = api.lookup_statuses(id=idlist)
            print('{}: {} statuses found'.format(ct, len(statuses)))
            for status in statuses:
                process(status)
            log = np.array([ct]).reshape(-1, 1)
            log = pd.DataFrame(log, columns=['epoch'])
            log.to_csv(folder_path + kw + '-log.csv', index=False, encoding='utf-8')
        ct+=1
    print('{} chunks in total!'.format(ct+1))



def hashtag_handler(path, hashtags, chunksize=100, process=save_to_persist):
    for chunk in pd.read_csv(path,
                             names='tweet_id',
                             chunksize=chunksize,
                             # nrows=100
                             ):
        idlist = [i.split('_')[-1] for i in chunk['tweet_id'].values]
        statuses = api.lookup_statuses(id=idlist)
        tot = len(statuses)
        statuses = [status for status in statuses if sum([hashtag in status.entities['hashtags'] for hashtag in hashtags]) > 0]
        print('{}/{} statuses containing {} found'.format(len(statuses), tot, hashtags))
        for status in statuses:
            process(status)

new_start = pd.read_csv(folder_path + kw + '-log.csv')
print('starting @:', int(new_start['epoch'].values[0]))
hydrator(folder_path+'IDDP_report_tweet_ids.csv', start_at=int(new_start['epoch'].values[0]))
