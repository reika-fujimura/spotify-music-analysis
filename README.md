# spotify-music-analysis


Music trend analysis with Spotify's API.

## Dataset

Spotify's weekly top 200 chart from 2017 to 2022 is available on [Spotify Charts by Dhruvil Dave on Kaggle datasets](https://www.kaggle.com/datasets/dhruvildave/spotify-charts). 

```
pd.read_csv(dpath, nrows=5)
```
![alt text](spotify-music-analyses/images/charts.png?raw=true)

With music url in chart dataset, connect music features which are offered by the Python library for the Spotify Web API (spotipy). Learn more about spotipy [here](https://spotipy.readthedocs.io/en/2.19.0/).

Music features

![alt text](spotify-music-analyses/images/features.png?raw=true)

>Acousticness: A measure of whether the track is acoustic.
>
>Danceability: Describes how suitable a track is for dancing.
>
>Energy: Represents a perceptual measure of intensity and activity. 
>
>Instrumentalness: Predicts whether a track contains no vocals.
>
>Liveness: Detects the presence of an audience in the recording.
>
>Speechiness: Detects the presence of spoken words in a track. 
>
>Tempo: The overall estimated tempo of a track in beats per minute (BPM). 
>
>Valence: Describes the musical positiveness conveyed by a track. 

## Data Collection

Creating dataset of specific country in 2022.
```
cd spotify-music-analyses/preprocess
python separate.py <COUNTRY NAME> <SUFFIX>
python preprocess.py <SUFFIX>
```

Specify year.
```
cd spotify-music-analyses/preprocess
python separate.py <COUNTRY NAME> <SUFFIX> --year <YEAR>
python preprocess.py <SUFFIX> --year <YEAR>
```
Dataset will be saved in `spotify-music-analyses/preprocess/data/<SUFFIX>/all.csv`.


## Results

See the detailed analysis and discusstions [here](https://github.com/reika-fujimura/spotify-music-analyses/tree/main/spotify-music-analyses//visualization/results.ipynv)!.

Full source code is available on [results.ipynb](https://github.com/reika-fujimura/spotify-music-analyses/tree/main/spotify-music-analyses//visualization/results.ipynv).

### Time-series variation of the average of each feature in US top200 songs

There is a big yearly trend around Christmas. 

![alt text](spotify-music-analyses/images/timeshift.png?raw=true)

### Features' distribution of top200 songs (Comparison of US and Canada)

Trends of US and Canada is quite simular.

![alt text](spotify-music-analyses/images/us_ca.png?raw=true)

### Features' distribution of top200 songs (Comparison of US and Japan)

There are big differences between the trend of US and Japan. Most listened songs in Japan tend to have less dansability, more energy, more loudness, less speachness, more variance, and longer duration time.

![alt text](spotify-music-analyses/images/us_jp.png?raw=true)

### Clustering

US Songs are clustered into 3 groups using k-means clustering method. 

Summary of three groups (group 0, group 1, group 2). See the detailed discusstions [here](https://github.com/reika-fujimura/spotify-music-analyses/tree/main/spotify-music-analyses//visualization/results.ipynv).

![alt text](spotify-music-analyses/images/clustering.png?raw=true)

## Sources

[Spotify Charts by Dhruvil Dave on Kaggle datasets](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)

[Spotify API](https://developer.spotify.com/documentation/web-api/)


