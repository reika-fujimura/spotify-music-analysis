import os
import logging
import argparse

import pandas as pd
import dask.dataframe as dd
import numpy as np


def separate_source_dataset(
    data_path: str, 
    save_dir: str,
    country: str,
    suffix: str,
    year: int
    ): 
    '''
    Extract the features of the songs on Spotify Chart Dataset from Spotify's API.
    Source: Spotify Chart from Kaggle Dataset
    https://www.kaggle.com/datasets/dhruvildave/spotify-charts
    '''
    save_dir = save_dir + suffix + '/' + str(year) + '/'
    os.makedirs(os.path.dirname(save_dir), exist_ok=True)
    logger.info('save dir: {}'.format(save_dir))

    usecols = ['title','rank','date','artist','url','region','chart','streams']
    datecol = ['date']
    keys = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 
            'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 
            'duration_ms', 'time_signature']
            
    df_dask = dd.read_csv(data_path, 
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
    def timestamp_to_month(timestamp):
        return timestamp.month
    def remove_comma(s):
        if type(s)!= str:
            return None
        return s.split(',')[0]

    df['year'] = df.date.apply(timestamp_to_year)
    df['month'] = df.date.apply(timestamp_to_month)
    df = df[df['year']==int(year)]
    df['title'] = df.title.apply(remove_comma)
    df['artist'] = df.artist.apply(remove_comma)
    
    for month in range(1,13):
        save_path = save_dir + '{}.csv'.format(str(month))
        tmp = df[df['month']==month].reset_index(drop=True)
        tmp.to_csv(save_path)
        logging.info('data size: {}'.format(tmp.shape))

    return None


if __name__ == '__main__':
    data_path = 'data/charts.csv'
    save_dir = 'data/sep/'

    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Merge datasets.')
    parser.add_argument('country', type=str, help='country name')
    parser.add_argument('suffix', type=str, help='save file suffix')
    parser.add_argument('--data_path', default=data_path, help='load path')
    parser.add_argument('--save_dir', default=save_dir, help='output directry')
    parser.add_argument('--year', default=2021, help='year')
    args = parser.parse_args()

    separate_source_dataset(
        data_path = args.data_path,
        save_dir = args.save_dir,
        country = str(args.country),
        suffix = str(args.suffix),
        year = args.year
    )


