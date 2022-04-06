import os
import logging
import argparse

import pandas as pd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from params import Client_ID, Client_Secret, SPOTIPY_REDIRECT_URI


def concat_dataset(
    dpath: str, 
    spath: str
    ): 
    '''
    Extract the features of the songs on Spotify Chart Dataset from Spotify's API.
    Source: Spotify Chart from Kaggle Dataset
    https://www.kaggle.com/datasets/dhruvildave/spotify-charts
    '''
    df = pd.read_csv(dpath)

    logging.info('data size: {}'.format(df.shape))

    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)
    l_url = df.url.to_list()

    keys = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 
            'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 
            'duration_ms', 'time_signature']
    features = np.ndarray((df.shape[0], len(keys)))

    for i in range(len(l_url)):
        url = l_url[i]
        d = sp.audio_features(url)[0]
        features[i,:] = [d[key] for key in keys]

    df[keys] = pd.DataFrame(features, columns = keys)
    df.save_csv(spath)

    return None


if __name__ == '__main__':
    data_path = 'data/charts.csv'
    save_path = 'data/features.csv'

    os.environ['SPOTIPY_CLIENT_ID'] = Client_ID
    os.environ['SPOTIPY_CLIENT_SECRET'] = Client_Secret
    os.environ['SPOTIPY_REDIRECT_URI'] = SPOTIPY_REDIRECT_URI

    parser = argparse.ArgumentParser(description='Merge datasets.')
    parser.add_argument('--dpath', default=data_path, help='load path')
    parser.add_argument('--spath', default=save_path, help='save path')
    args = parser.parse_args()

    concat_dataset(
        dpath = args.dpath,
        spath = args.spath
    )
