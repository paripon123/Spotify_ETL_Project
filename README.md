# Spotify_ETL_Project
Building an ETL from Spotify API and a weekly summary email report


### Motivation
The goal of this project is to build a whole pipeline of extracting data from Spotify API transform and load to local database, and generate weekly report send to my email. I’m extracting historical data from my personal Spotify account. Then, I applied pandas to do transformation and cleaning, processing, and load into a PostgreSQL database (Local Machine). Moreover, I utilized SQL and python(SQLAlchemy) to query metric information and send to my email weekly by using Airflow to trigger a schedule

### Pipeline

![image](https://user-images.githubusercontent.com/51777237/129422852-6ac3ed29-1018-45ce-8ad1-d93d964fb027.png)

## ETL Process (Extract, Transform, Load)
#### _Extracting_
I utilized Spotipy package to extract my personal data from Spotify through Web API endpoints. The Data I collected including JSON metadata about music, artist, albums, and tracks. 

#### _Transforming_
From extracting data through Python, I applied pandas to transform all the data and put it into data frame format. Then load in to PosgreSQL database
	List of Transformation
		- Transformed and put it in to DataFrame format
			-  Albums Data
			-  Tracks Data
			-  Artist Data
			-  Feature Data
		- Create Unique Identifier for each song played
		- Converted date-time format
		- Generate a bridge dataset including unique id and metric call Fact table
#### _Loading_
First, creating databases on my local machine using PostgreSQL. Five tables were created including Album, Song, Feature, Artist and Fact to ingest data from transformation. I used .to_sql which is a function In Pandas to transform DataFram into SQL statement and load to PosgreSQL

  ###### Data Modeling
![image](https://user-images.githubusercontent.com/51777237/129423341-ba225070-4191-4f35-a50e-97cc1d9cc37f.png)
    
## Weekly Summary Report
The weekly report is incorporate metrics from Spotify Wrapped yearly round up. I utilized SQL to generate the report such as Top 5 song played, Top 5 artists. Moreover, the email was sent directly from python script.

#### _Airflow_
Apache-Airflow was applied to ETL and Email Weekly python script to schedule a weekly run.





