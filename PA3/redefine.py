from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.Spotify

# Check connection by listing databases
# try:
#     database_list = client.list_database_names()
#     print("Connected to MongoDB! Databases:", database_list)
# except Exception as e:
#     print("Error connecting to MongoDB:", e)

# Test the connection by listing collections
# try:
#     collections = db.list_collection_names()
#     if collections:
#         print("Connection successful!")
#         print("Collections in the database:", collections)
#     else:
#         print("Connection successful, but the database has no collections.")
# except Exception as e:
#     print("Failed to connect to MongoDB:", str(e))



# Correct collection names
playlist_collection = db['playlist']
playlist_song_collection = db['playlist_song']
playlist_follower_collection = db['playlist_follower']

# Create or clean the new collection
new_playlist_collection = db['playlist_embedded']
new_playlist_collection.drop()  # Drop existing data to avoid duplicates

# Iterate through all playlists
for playlist in playlist_collection.find():
    playlist_id = playlist["id"]  # Use 'id' field instead of '_id'

    print(f"\nProcessing playlist: {playlist['name']} (ID: {playlist_id})")

    # Fetch and embed songs using 'playlist_id'
    playlist_song = playlist_song_collection.find({"playlist_id": playlist_id})
    embedded_songs = []
    for song in playlist_song:
        embedded_songs.append({
            "song_id": song["song_id"],
            "position": song["position"]
        })
    print(f"  Embedded {len(embedded_songs)} songs.")

    # Fetch and embed followers using 'playlist_id'
    follower = playlist_follower_collection.find({"playlist_id": playlist_id})
    embedded_followers = [follower["follower_id"] for follower in follower]
    print(f"  Embedded {len(embedded_followers)} followers.")

    # Add embedded data to the playlist document
    playlist["songs"] = embedded_songs
    playlist["followers"] = embedded_followers

    # Insert into the new collection
    new_playlist_collection.insert_one(playlist)

print("Daten erfolgreich übertragen!")