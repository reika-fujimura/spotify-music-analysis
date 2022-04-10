import os
import logging
import argparse
import multiprocessing as mp
import csv

import pandas as pd
import dask.dataframe as dd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from utils.params import Client_ID, Client_Secret


def create_feature_data(
    df_chunk: pd.DataFrame, 
    save_path: str
    ):  
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)
    keys = ['danceability', 'energy', 'key', 'loudness', 'mode', 
            'speechiness', 'acousticness', 'instrumentalness', 
            'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']
    
    logger.info('2. Started process to the save path {}.'.format(save_path))
    
    features = np.ndarray((df_chunk.shape[0], len(keys)))
    urls = list(df_chunk['url'])

    for i in range(df_chunk.shape[0]):
        url = urls[i]
        auth_manager = SpotifyClientCredentials()
        sp = spotipy.Spotify(auth_manager=auth_manager)
        d = sp.audio_features(url)[0]
        features[i,:] = [d[key] for key in keys]
        
    df_chunk = pd.concat([df_chunk, pd.DataFrame(features, columns = keys)], axis=1)
    df_chunk.to_csv(save_path)
    logger.info('3. Ended process and saved in {}.'.format(save_path))

def create_feature_data_wrapper(args):
    return create_feature_data(*args)

def feature_multiprocessing(
    df: pd.DataFrame, 
    save_dir: str, 
    pool_size: int, 
    csv_size: int
    ):
    pool = mp.Pool(pool_size)
    chunk_size = csv_size // pool_size
    rem = csv_size % pool_size
    values = []
    start_line = 0
    for i in range(pool_size):
        logger.info('1. Process start: thread num: {0}, start_line: {1}'.format(i, start_line))
        save_path = save_dir + '{}'.format(i) + '.csv'
        if i <= rem:
            end_line = start_line + chunk_size + 1
            values.append((df.iloc[start_line:end_line, :].reset_index(drop=True), save_path))
            start_line += chunk_size + 1
        else:
            end_line = start_line + chunk_size
            values.append((df.iloc[start_line:end_line, :].reset_index(drop=True), save_path))
            start_line += chunk_size 
    
    pool.map(create_feature_data_wrapper, values)
    pool.close() # clear memory
    pool.join() # wait for all processes

def concat_dataset(
    data_path: str, 
    save_dir: str,
    pool_size: int,
    country: str,
    suffix: str,
    year: int
    ): 
    '''
    Extract the features of the songs on Spotify Chart Dataset from Spotify's API.
    Source: Spotify Chart from Kaggle Dataset
    https://www.kaggle.com/datasets/dhruvildave/spotify-charts
    '''
    usecols = ['title','rank','date','artist','url','region','chart','streams']
    datecol = ['date']
    keys = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 
            'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 
            'duration_ms', 'time_signature']
            
    df_dask = dd.read_csv('data/charts.csv', 
                        usecols=usecols,
                        parse_dates=datecol,
                        dtype={
                            'title':'str',
                            'rank':'float64',
                            'date':'str',
                            'artist':'str',
                            'url':'str',
                            'region':'category',
                            'chart':'category',
                            'streams':'float64'
                            }
                        )
    df_dask = df_dask.query("chart=='top200'")
    df_dask = df_dask[df_dask['region']==country]

    df = df_dask.compute() # convert to pandas dataframe

    def timestamp_to_year(timestamp):
        return timestamp.year
    df['year'] = df.date.apply(timestamp_to_year)
    df = df[df['year']==year].drop(columns='year')

    logger.info('Data size: {}'.format(df.shape))

    csv_size = df.shape[0]
    save_dir = save_dir + str(year) + '/'
    os.makedirs(os.path.dirname(save_dir), exist_ok=True)
    
    feature_multiprocessing(df, save_dir, pool_size, csv_size)

    return None


if __name__ == '__main__':
    data_path = 'data/charts.csv'
    save_dir = 'data/tmp/'

    os.environ['SPOTIPY_CLIENT_ID'] = Client_ID
    os.environ['SPOTIPY_CLIENT_SECRET'] = Client_Secret

    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Merge datasets.')
    parser.add_argument('country', type=str, help='country name')
    parser.add_argument('suffix', type=str, help='save file suffix')
    parser.add_argument('--data_path', default=data_path, help='load path')
    parser.add_argument('--save_dir', default=save_dir, help='output directry')
    parser.add_argument('--pool_size', default=100, help='multiprocessing core number')
    parser.add_argument('--year', default=2021, help='year')
    args = parser.parse_args()

    concat_dataset(
        data_path = args.data_path,
        save_dir = args.save_dir,
        pool_size = args.pool_size,
        country = args.country,
        suffix = args.suffix,
        year = args.year
    )
