import pymysql
from pymongo import MongoClient
from datetime import datetime, date

# MySQL Verbindung
mysql_connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Parkash@2001',
    db='scenario_spotify'
)
mysql_cursor = mysql_connection.cursor()

# MongoDB Verbindung
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['Spotify']

# Funktion zum Abrufen von Songs
def get_songs(playlist_id):
    mysql_cursor.execute("""
        SELECT song.id, song.title, song.artist, playlist_song.position 
        FROM playlist_song 
        JOIN song ON playlist_song.song_id = song.id 
        WHERE playlist_song.playlist_id = %s 
        ORDER BY playlist_song.position
    """, (playlist_id,))
    return [{"song_id": str(song[0]), "title": song[1], "artist": song[2], "position": song[3]} for song in mysql_cursor.fetchall()]

# Funktion zum Abrufen von Followern
def get_followers(playlist_id):
    mysql_cursor.execute("""
        SELECT user.id, user.name 
        FROM playlist_follower 
        JOIN user ON playlist_follower.follower_id = user.id 
        WHERE playlist_follower.playlist_id = %s
    """, (playlist_id,))
    return [{"follower_id": str(follower[0]), "follower_name": follower[1]} for follower in mysql_cursor.fetchall()]

# Abrufen und Speichern von Benutzern
mysql_cursor.execute("SELECT * FROM user")
users = mysql_cursor.fetchall()

for user in users:
    user_id, name, gender = user

    # Speichern des Benutzers in MongoDB
    mongo_db.users.insert_one({
        "name": name,
        "gender": gender,
    })

# Abrufen und Speichern von Songs
mysql_cursor.execute("SELECT * FROM song")
songs = mysql_cursor.fetchall()

for song in songs:
    song_id, title, artist = song

    # Speichern des Songs in MongoDB
    mongo_db.songs.insert_one({
        "title": title,
        "artist": artist,
    })

# Abrufen und Speichern von Playlists
mysql_cursor.execute("SELECT * FROM playlist")
playlists = mysql_cursor.fetchall()

for playlist in playlists:
    playlist_id, playlist_name, created_date, owner_id = playlist

    # Konvertiere created_date in datetime.datetime
    if isinstance(created_date, date):
        created_date = datetime.combine(created_date, datetime.min.time())

    followers = get_followers(playlist_id)
    songs = get_songs(playlist_id)

    # Speichern der Playlist in MongoDB
    mongo_db.playlists.insert_one({
        "name": playlist_name,
        "created_date": created_date,
        "owner_id": str(owner_id),  # Referenz auf User
        "followers": [str(follower['follower_id']) for follower in followers],
        "songs": [{"song_id": str(song['song_id']), "position": song['position']} for song in songs]
    })

# Schließen der Verbindungen
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()

print("Datenmigration abgeschlossen.")