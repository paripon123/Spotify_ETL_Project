import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
import json
from datetime import datetime, timedelta

def weekly_email():
    # connect to postgres
    conn = psycopg2.connect(host = 'localhost', port = '5432', dbname ='spotify',user = 'pariponthanthong')
    cur = conn.cursor()
    today = datetime.today().date()
    six_day_ago = today - timedelta(days = 6)

    # Top 5 Songs by most listend(most count)
    top_5_song_count = [['Song Name',  'Count']]
    cur.callproc('spotify_schemas.get_artist_played_last_7days')
    for row in cur.fetchall():
        song_name = row[0]
        times_listened = row[1]
        element = [song_name, times_listened]
        top_5_song_count.append(element)

    # Top 5 Time Listened (Hour)
    cur.callproc('spotify_schemas.get_total_time_listened_hrs_last_7days')
    total_time_listened_hrs = float(cur.fetchone()[0])
    
    # Top 5 song and Artist by Times Played
    total_song_art_played = [['Song Name','Artist Name','Times Played']]
    cur.callproc('spotify_schemas.get_song_artist_most_popular_last_7days')
    for row in cur.fetchall():
        song_name = row[0]
        artist_name = row[1]
        times_played = row[2]
        element = [song_name,artist_name,times_played]
        total_song_art_played.append(element)

    # Top Artist Played
    top_artist_played = [['Artist Name','Times Played']]
    cur.callproc('spotify_schemas.get_artist_played_last_7days')
    for row in cur.fetchall():
        artist_name = row[0]
        times_played = int(row[1])
        element = [artist_name,times_played]
        top_artist_played.append(element)

    #Top Decade
    top_decade_played = [['Decade', 'Times Played']]
    cur.callproc('spotify_schemas.get_top_decades_last_7days')
    for row in cur.fetchall():
        decade = row[0]
        times_played = row[1]
        element = [decade,times_played]
        top_decade_played.append(element)

    ###
    # Sending Email
    ###

    port = 465
    password = "popersers12"

    sender_email = "testparipon@gmail.com"
    receiver_email = "paripon.thanthong@gmail.com"
    
    message = MIMEMultipart("alternative")
    message["Subject"] = f"Spotify Weekly Roundup - {today}"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"""\
    Here are the stats for weekly round up for Spotyfy.
    Date Included: {six_day_ago} - {today}:

    Total Time Listened: {total_time_listened_hrs} hours.
    You listened to this songs and artist quite a lot, here is your top5!
    {total_song_art_played}
    You spent most of the time listening to these songs:
    {top_5_song_count}
    You spent the most time listening to these artists:
    {top_artist_played}
    Finally, most of the song you listen is in this era!!!
    {top_decade_played}
    """

    html = f"""\
    <html>
        <body>
            <h4>
            Here are the stats for weekly round up for Spotyfy.
            </h4>
            <p>
            Date Included: {six_day_ago} - {today}:
            <br>
            Total Time Listened: {total_time_listened_hrs} hours.
            <br>
            <h4>
            You listened to this songs and artist quite a lot, here is your top5!
            </h4>
            {tabulate(total_song_art_played, tablefmt = 'html')}
            <h4>
            You spent most of the time listening to these songs:
            </h4>
            {tabulate(top_5_song_count,tablefmt = 'html')}
            <h4>
            You spent the most time listening to these artists:
            </h4>
            {tabulate(top_artist_played,tablefmt = 'html')}
            <h4>
            Finally, most of the song you listen is in this era!!!
            </h4>
            {tabulate(top_decade_played,tablefmt = 'html')}
            </p>
        </body>
    </html>
    """

    part1 = MIMEText(text,'plain')
    part2 = MIMEText(html,'html')

    message.attach(part1)
    message.attach(part2)

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com",port, context = context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email,receiver_email,message.as_string())

    return "Email Sent Successful"
