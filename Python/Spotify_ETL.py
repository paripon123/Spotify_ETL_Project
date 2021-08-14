import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
from pprint import pprint

def spotify_ETL_func():
    client_id = '' # Generate on spotify development
    client_secret = '' # Generate on spotify development
    redirect_url = '' # Pick You Local Host

    sp = spotipy.Spotify(auth_manager = SpotifyOAuth(client_id = client_id,
                                                    client_secret = client_secret,
                                                    redirect_uri = redirect_url,
                                                    scope = "user-read-recently-played"))

    recently_played = sp.current_user_recently_played(limit = 50)

    if len(recently_played) == 0:
        sys.exit("No record recieved from Spotify")


    # Spotify Acess to get genre
    client_credentials_manager = SpotifyClientCredentials(client_id,client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Creating Album Data Structure
    album_list = []
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_track = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_detail = {
                        'album_id': album_id,
                        'name': album_name,
                        'release_date': album_release_date,
                        'total_tracks': album_total_track,
                        'url': album_url}
        album_list.append(album_detail)

    #Create Artist Data Structure
    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    genre_list = []
    for item in recently_played['items']:
        for key,value in item.items():
            if key == "track":
                for data in value['artists']:
                    id_list.append(data['id'])
                    name_list.append(data['name'])
                    url_list.append(data['external_urls']['spotify'])

    artist_dict = {'artist_id':id_list,'name':name_list,'url':url_list}

    tracks_list = []
    for row in recently_played['items']:
        track_id = row['track']['id']
        track_name = row['track']['name']
        track_duration = row['track']['duration_ms']
        track_popularity = row['track']['popularity']
        played_at = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        track_detail = {
                        'track_id':track_id,
                        'track_name':track_name,
                        'track_duration': track_duration,
                        'track_popularity': track_popularity,
                        'date_time_played': played_at,
                        'album_id': album_id,
                        'artist_id': artist_id
        }
        tracks_list.append(track_detail)

    #Album
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset = ['album_id'])

    #Artists
    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    # Add Genere
    genre = []
    for name in artist_df['name']:
        result = sp.search(name)
        track = result['tracks']['items'][0]
        artist = sp.artist(track['artists'][0]['external_urls']['spotify'])
        artist_genre = artist['genres']
        artist_genre = ', '.join(artist_genre)
        genre.append(artist_genre)
    artist_df['genre'] = genre

    #Song
    song_df = pd.DataFrame.from_dict(tracks_list)
    #Change date_time_plated to timestamp
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    #song_df.info()
    #Convert time zone to US/Central
    song_df['date_time_played'] = song_df['date_time_played'].dt.tz_convert('US/Central')
    #Remove timezone part
    song_df['date_time_played'] = song_df['date_time_played'].astype(str).str[:-7]
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])

    #Create UNIX timestamp
    song_df['UNIX_Time_Stamp'] = (song_df['date_time_played'] - pd.Timestamp('1970-01-01')) // pd.Timedelta('1s')

    #Generate Unique Identifier
    song_df['unique_identifier'] = song_df['track_id'] + song_df['UNIX_Time_Stamp'].astype(str)

    song_df = song_df[['track_id', 'track_name', 'track_duration', 'track_popularity',
        'date_time_played','unique_identifier']]

    # TEST
    #song_df.to_excel('test.xlsx', index = False)

    # Song's Feature
    feature = []
    for id in song_df['track_id']:
        features = sp.audio_features(id)
        for i in features:
            feature.append(i)
    feature_df = pd.DataFrame(feature)
    feature_df['feature_id'] = feature_df['id'] 
    feature_df = feature_df.drop(['type','uri','track_href','analysis_url','duration_ms','time_signature','id'], axis =1)
    feature_df = feature_df.drop_duplicates(subset=['feature_id'])

    # Fact Table (Bridge Table)
    fact_df = song_df[['unique_identifier']]
    fact_df['album_id'] = album_df[['album_id']]
    fact_df['artist_id'] = artist_df['artist_id']
    fact_df['feature_id'] = feature_df[['feature_id']]
    fact_df['duration_played'] = song_df[['track_duration']]
    fact_df['loudness'] = feature_df[['loudness']]
    fact_df['genre'] = artist_df[['genre']]

    # 
    conn = psycopg2.connect(host = '', port = '', dbname ='spotify') # Chose you port , host
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://""/spotify') # Need to include username+password > Lookup sqlAlchemy doc
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()

    #Song
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS temp_song as SELECT * FROM spotify_schemas.song LIMIT 0
    """)
    song_df.to_sql('temp_song', con=engine, if_exists = 'append', index = False)

    cur.execute(
    """
    INSERT INTO spotify_schemas.song
    SELECT temp_song.*
    FROM temp_song
    LEFT JOIN spotify_schemas.song USING (unique_identifier)
    WHERE spotify_schemas.song.unique_identifier IS NULL;

    DROP TABLE temp_song
    """)
    conn.commit()

    #Album
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS temp_album as SELECT * FROM spotify_schemas.album LIMIT 0
    """)
    album_df.to_sql('temp_album', con = engine, if_exists = 'append', index = False)
    cur.execute(
    """
    INSERT INTO spotify_schemas.album
    SELECT temp_album.*
    FROM temp_album
    LEFT JOIN spotify_schemas.album USING (album_id)
    WHERE spotify_schemas.album.album_id IS NULL;

    DROP TABLE temp_album
    """)
    conn.commit()

    #Artist


    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS temp_artist as SELECT * FROM spotify_schemas.artist LIMIT 0
    """)
    artist_df.to_sql('temp_artist', con = engine, if_exists = 'append', index = False)
    cur.execute(
    """
    INSERT INTO spotify_schemas.artist
    SELECT temp_artist.*
    FROM temp_artist
    LEFT JOIN spotify_schemas.artist USING (artist_id)
    WHERE spotify_schemas.artist.artist_id IS NULL;

    DROP TABLE temp_artist
    """)
    conn.commit()

    # Feature
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS temp_feature as SELECT * FROM spotify_schemas.feature LIMIT 0
    """)
    feature_df.to_sql('temp_feature', con = engine, if_exists = 'append', index = False)

    cur.execute(
    """
    INSERT INTO spotify_schemas.feature
    SELECT temp_feature.*
    FROM temp_feature
    LEFT JOIN spotify_schemas.feature USING (feature_id)
    WHERE spotify_schemas.feature.feature_id IS NULL;

    DROP TABLE temp_feature
    """)
    conn.commit()


    #Fact table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS temp_fact as SELECT * FROM spotify_schemas.fact LIMIT 0
    """)
    fact_df.to_sql('temp_fact', con = engine, if_exists = 'append', index = False)
    cur.execute(
    """
    INSERT INTO spotify_schemas.fact
    SELECT temp_fact.*
    FROM temp_fact
    LEFT JOIN spotify_schemas.fact USING (unique_identifier,album_id,artist_id,feature_id)
    WHERE spotify_schemas.fact.unique_identifier IS NULL or
        spotify_schemas.fact.album_id IS NULL or
        spotify_schemas.fact.artist_id IS NULL or
        spotify_schemas.fact.feature_id IS NULL;

    DROP TABLE temp_fact
    """)
    conn.commit()

    return 'ETL Process Successul!!'

