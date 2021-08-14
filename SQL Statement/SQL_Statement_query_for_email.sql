-- Create functions to run these queries for my weekly email that shows the statistics.
-- Creating Function allow me to have cleaner code when writting the email which is a perfect use case for them.
-- Function and Query

-- TOP 5 songs in the last 7 days by most count
-- QUERY
SELECT s.track_name as track , count(s.track_name) as most_listened
FROM spotify_schemas.song s
WHERE s.date_time_played > CURRENT_DATE - interval '7 days'
GROUP by s.track_name
ORDER BY most_listened DESC limit 5;

-- Function 
CREATE FUNCTION get_most_listened_last_7days()
RETURNS table (track text, most_listened int) language plpgsql as $$

BEGIN 
    RETURN query
        SELECT s.track_name as track , count(s.track_name) as most_listened
        FROM spotify_schemas.song s
        WHERE s.date_time_played > CURRENT_DATE - interval '7 days'
        GROUP by s.track_name
        ORDER BY most_listened DESC limit 5;

end;$$

-- Total Time listen in the last 7 days

SELECT ROUND(sum(CAST( s.track_duration as decimal)/3600000),2) as total_time_listened_hrs
FROM spotify_schemas.song s
WHERE date_time_played > current_date - interval '7 days';

-- Function

CREATE FUNCTION get_total_time_listened_hrs_last_7days()
RETURNS table (total_time_listened_hrs decimal) language plpgsql as $$

BEGIN 
    RETURN query
    SELECT ROUND(sum(CAST( s.track_duration as decimal)/3600000),2) as total_time_listened_hrs
    FROM spotify_schemas.song s
    WHERE date_time_played > current_date - interval '7 days';

end;$$


-- Most Popular song and artist_name by number of plays
--QUERY

SELECT  ss.track_name, sa.name AS Artist_name , count(ss.*) :: INT as times_played
FROM spotify_schemas.fact sf
    JOIN spotify_schemas.song ss
    ON sf.unique_identifier= ss.unique_identifier
    JOIN spotify_schemas.artist sa
    ON sf.artist_id = sa.artist_id
WHERE ss.date_time_played > current_date - interval '7 days'
GROUP BY ss.track_name, sa.name
ORDER BY times_played DESC 
LIMIT 10;

--Function

CREATE FUNCTION get_song_artist_most_popular_last_7days()
RETURNS table (track_name TEXT, Artist_name TEXT, times_played INT) language plpgsql as $$

BEGIN 
    RETURN query
    SELECT  ss.track_name, sa.name AS Artist_name , count(ss.*) :: INT as times_played
    FROM spotify_schemas.fact sf
        JOIN spotify_schemas.song ss
        ON sf.unique_identifier= ss.unique_identifier
        JOIN spotify_schemas.artist sa
        ON sf.artist_id = sa.artist_id
    WHERE ss.date_time_played > current_date - interval '7 days'
    GROUP BY ss.track_name, sa.name
    ORDER BY times_played DESC 
    LIMIT 10;
end;$$


-- Top Artist Listened To
--QUERY

SELECT sa.name, count(ss.*):: INT as number_of_song_played
    FROM spotify_schemas.fact sf
        JOIN spotify_schemas.song ss
        ON sf.unique_identifier= ss.unique_identifier
        JOIN spotify_schemas.artist sa
        ON sf.artist_id = sa.artist_id
WHERE ss.date_time_played > current_date - interval '7 days'
GROUP BY sa.name
ORDER BY number_of_song_played DESC
LIMIT 10;

-- Function
CREATE FUNCTION get_artist_played_last_7days()
RETURNS table (name TEXT, number_of_song_played INT) language plpgsql as $$

BEGIN 
    RETURN query
    SELECT sa.name, count(ss.*):: INT as number_of_song_played
    FROM spotify_schemas.fact sf
        JOIN spotify_schemas.song ss
        ON sf.unique_identifier= ss.unique_identifier
        JOIN spotify_schemas.artist sa
        ON sf.artist_id = sa.artist_id
    WHERE ss.date_time_played > current_date - interval '7 days'
    GROUP BY sa.name
    ORDER BY number_of_song_played DESC
    LIMIT 10;

end;$$


-- Create View:
CREATE OR REPLACE VIEW track_decades AS
SELECT *,
CASE 
WHEN subqry.release_year >= 1950 AND subqry.release_year <= 1959  THEN '1950''s'
WHEN subqry.release_year >= 1960 AND subqry.release_year <= 1969  THEN '1960''s'
WHEN subqry.release_year >= 1970 AND subqry.release_year <= 1979  THEN '1970''s'
WHEN subqry.release_year >= 1980 AND subqry.release_year <= 1989  THEN '1980''s'
WHEN subqry.release_year >= 1990 AND subqry.release_year <= 1999  THEN '1990''s'
WHEN subqry.release_year >= 2000 AND subqry.release_year <= 2009  THEN '2000''s'
WHEN subqry.release_year >= 2010 AND subqry.release_year <= 2019  THEN '2010''s'
WHEN subqry.release_year >= 2020 AND subqry.release_year <= 2029  THEN '2020''s'
WHEN subqry.release_year >= 2030 AND subqry.release_year <= 2039  THEN '2030''s'
WHEN subqry.release_year >= 2040 AND subqry.release_year <= 2049  THEN '2040''s'
ELSE 'Other'
END AS decade
FROM
(
    SELECT album.album_id, album.name, album.release_date, song.unique_identifier, song.date_time_played, song.track_name, CAST(SPLIT_PART(release_date,'-',1) AS INT) as release_year
    FROM spotify_schemas.fact as sf
    INNER JOIN spotify_schemas.album as album ON sf.album_id = album.album_id
    INNER JOIN spotify_schemas.song as song on sf.unique_identifier= song.unique_identifier
) AS subqry;

-- QUERY
SELECT decade, COUNT(unique_identifier) AS total_played
FROM track_decades
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY decade
ORDER BY decade DESC


-- Function
CREATE FUNCTION get_top_decades_last_7days()
RETURNS table (decade TEXT, total_played INT) language plpgsql as $$

BEGIN 
    RETURN query
    SELECT track_decades.decade, COUNT(unique_identifier)::INT AS total_played
    FROM track_decades
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY track_decades.decade
    ORDER BY total_played DESC;

end;$$


