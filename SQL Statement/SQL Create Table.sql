-- PostGres SQL Statement.
-- Create table


-- Create Schemas to wrap all table in on specific schemas
CREATE schema spotify_schemas;

-- Creating Track Table

CREATE TABLE IF NOT EXISTS spotify_schemas.song(
	track_id TEXT NOT NULL,
	track_name TEXT,
	track_duration INT,
	track_popularity INT, 
date_time_played TIMESTAMP,
unique_identifier TEXT PRIMARY KEY NOT NULL,
	date_time_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Creating Album Table

CREATE TABLE IF NOT EXISTS spotify_schemas.album (
	album_id TEXT PRIMARY KEY NOT NULL,
	name TEXT,
	release_date TEXT,
	total_tracks SMALLINT,
	url TEXT
);

-- Creating Artist Table

CREATE TABLE IF NOT EXISTS spotify_schemas.artist(
	Artist_id TEXT PRIMARY KEY NOT NULL,
	Name TEXT,
	url TEXT,
	genre TEXT
);
	
-- Creating Feature Table

CREATE TABLE IF NOT EXISTS spotify_schemas.feature(
	Danceability DECIMAL,
	Energy DECIMAL,
	Key INT,
	Loudness DECIMAL,
 	Mode INT,
 	Speechiness DECIMAL,
 	Acousticness DECIMAL, 
	Instrumentalness DECIMAL,
 	Liveness DECIMAL, 
	Valence DECIMAL, 
	Tempo DECIMAL,
    feature_id TEXT PRIMARY KEY NOT NULL
);

-- Creating Fact Table

CREATE TABLE IF NOT EXISTS spotify_schemas.fact(
	unique_identifier TEXT REFERENCES spotify_schemas.song (unique_identifier),
album_id TEXT REFERENCES spotify_schemas.album(album_id), 
artist_id TEXT REFERENCES spotify_schemas.artist (artist_id),
	Feature_id TEXT REFERENCES spotify_schemas.feature (Feature_id),
	duration_played int,
	loudness DECIMAL,
	genre TEXT
);
