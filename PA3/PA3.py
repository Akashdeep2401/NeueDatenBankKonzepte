import datetime
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
#from graphene import ObjectType, String, List, Schema
#from graphene_mongo import MongoengineObjectType
from flask_graphql import GraphQLView
from pymongoexplain import ExplainCollection

PA3 = Flask(__name__)

# MongoDB Konfiguration
PA3.config['MONGO_URI'] = 'mongodb://localhost:27017/Spotify'
mongo = PyMongo(PA3)


@PA3.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "running"})

# Test the MongoDB connection
@PA3.route('/test_db_connection', methods=['GET'])
def test_db_connection():
    try:
        # Perform a simple query to check the connection
        mongo.db.users.find_one()
        return jsonify({'message': 'Database connection successful!'})
    except Exception as e:
        return jsonify({'message': 'Database connection failed:', 'error': str(e)})

# Collections for Songs, Users, Playlists, PlaylistSongs, PlaylistFollowers
songs_collection = mongo.db.song
users_collection = mongo.db.user
playlists_collection = mongo.db.playlist
playlist_songs_collection = mongo.db.playlist_song
playlist_followers_collection = mongo.db.playlist_follower


# region Anforderung 1: Song Ausgabe

@PA3.route('/songs', methods=['GET'])
def get_songs():
    #filters
    title_filter = request.args.get('title')
    artist_filter = request.args.get('artist')

    #query
    query = {}
    if title_filter:
        query['title'] = {'$regex': title_filter, '$options': 'i'} # case-insensitive
    if artist_filter:
        query['artist'] = {'$regex': artist_filter, '$options': 'i'}

    # Fetch the query execution plan using explain()
    try:
        explain_result = songs_collection.find(query).explain()    
        print("EXPLAIN Results:")
        print(explain_result)  # Print the detailed execution plan to the console
    except Exception as e:
        print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")

    #Paginierung
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    skip_count = (page - 1) * per_page

    #fetch songs from MongoDB
    songs = songs_collection.find(query).skip(skip_count).limit(per_page)
    songs_list = list(songs)

    #check if songs exist
    if not songs_list:
        return jsonify({'message': 'Kein Song gefunden mit den gegebenen Filtern.'})
    
    #return songs
    songs_json = [
        {
            'id': str(song['_id']),
            'title': song['title'],
            'artist': song['artist']
        }
        for song in songs_list
    ]

    #total number of songs
    total_songs = songs_collection.count_documents(query)
    total_pages = (total_songs +per_page -1) // per_page

    return jsonify({
        'songs': songs_json,
        'total_songs': total_songs,
        'total_pages': total_pages,
        'current_page': page
    })

# Alle Songs:
# http://127.0.0.1:5000/songs
# Nach Song-Titel sortiert (vice versa für Artist):
# http://127.0.0.1:5000/songs?title=love&page=1&per_page=5
# Song konnte nicht gefunden werden:
# http://127.0.0.1:5000/songs?artist=shreya&page=1&per_page=5

# endregion


# region Anforderung 2: Song hinzufügen

@PA3.route('/songs/add', methods=['POST'])
def add_song():
    #get song data from request
    data= request.get_json()

    #check if the title and artist are provided
    if not data or 'title' not in data or 'artist' not in data:
        return jsonify({'message': 'Titel und Artist sind erforderlich!'}), 400
    
    #add song to MongoDB

    new_song = {
        'title': data['title'],
        'artist': data['artist']
    }

    #insert song into MongoDB
    try:
        result = songs_collection.insert_one(new_song)
        return jsonify({'message': 'Song wurde erfolgreich hinzugefügt!', 'id': str(result.inserted_id)}), 201
    except Exception as e:
        mongo.session.rollback()
        return jsonify({'message': 'Fehler beim Hinzufügen des Songs!', 'error': str(e)}), 400

# curl -Uri http://127.0.0.1:5000/songs/add -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"title": "Akash new song", "artist": "Akash"}'
# {artist: 'Akash'}

# endregion

# region Update Song

@PA3.route('/songs/<string:song_id>', methods=['PUT'])
def update_song(song_id):
    data = request.get_json()

    #check if the title and artist are provided
    if not data or 'title' not in data or 'artist' not in data:
        return jsonify({'message': 'Titel und Artist sind erforderlich!'}), 400
    
    song = songs_collection.find_one({'_id': ObjectId(song_id)})
    if not song:
        return jsonify({'message': 'Song nicht gefunden!'}), 404
    
    # change title or artist
    update_data ={}
    if 'title' in data:
        update_data['title'] = data['title']
    if 'artist' in data:
        update_data['artist'] = data['artist']
    
    #update song in MongoDB
    try:
        songs_collection.update_one({'_id': ObjectId(song_id)}, {'$set': update_data})
        return jsonify({'message': 'Song erfolgreich aktualisiert!'}), 200
    except Exception as e:
        return jsonify({'message': 'Fehler beim Aktualisieren des Songs!', 'error': str(e)}), 400

# curl -Uri http://127.0.0.1:5000/songs/674a2a065f429f56a0dd3e55 -Method Put -Headers @{"Content-Type"="application/json"} -Body '{"title": "Updated Song Title Akash", "artist": "Updated Artist Akash"}'  

# endregion

# region Delete Song

@PA3.route('/songs/delete/<string:song_id>', methods=['DELETE'])
def delete_song(song_id):
    result = songs_collection.delete_one({'_id': ObjectId(song_id)})
    
    if result.deleted_count == 0:
        return jsonify({'message': 'Song nicht gefunden!'}), 404
    
    try:
        playlist_songs_collection.delete_many({'song_id': ObjectId(song_id)})
        return jsonify({'message': 'Song erfolgreich gelöscht!'}), 200
    except Exception as e:
        return jsonify({'message': 'Fehler beim Löschen des Songs!', 'error': str(e)}), 400


# endregion

# region Anforderung 3: Playlist Ausgabe

@PA3.route('/playlists', methods=['GET'])
def get_playlists():
    name_filter = request.args.get('name')
    
    explain_collection = ExplainCollection(playlists_collection)  # Wrap the collection with ExplainCollection
    # Start aggregation pipeline
    pipeline = []

    # Filter by playlist name if provided
    if name_filter:
        pipeline.append({
            '$match': {
                'name': {'$regex': name_filter, '$options': 'i'}  # Case-insensitive search for playlist name
            }
        })


    # Lookup User to get owner details
    pipeline.append({
        '$lookup': {
            'from': 'user',               # Correct collection name: 'user'
            'localField': 'owner_id',     # Local field in 'playlists' collection
            'foreignField': 'id',         # Corresponding field in 'user' collection
            'as': 'owner'                 # Output the joined data in a new field called 'owner'
        }
    })

    # Lookup PlaylistSongs to get the songs in each playlist
    pipeline.append({
        '$lookup': {
            'from': 'playlist_song',     # Correct collection name: 'playlist_song'
            'localField': 'id',          # Local field in 'playlists' collection
            'foreignField': 'playlist_id', # Corresponding field in 'playlist_song' collection
            'as': 'songs'                # Output the joined data in a new field called 'songs'
        }
    })

    # Lookup Songs to get song details from 'song' collection
    pipeline.append({
        '$lookup': {
            'from': 'song',              # Correct collection name: 'song'
            'localField': 'songs.song_id', # Local field in 'playlist_song' (songs)
            'foreignField': 'id',         # Corresponding field in 'song' collection
            'as': 'song_details'          # Add song details to the field
        }
    })

    # Lookup PlaylistFollowers to count distinct followers for each playlist
    pipeline.append({
        '$lookup': {
            'from': 'playlist_follower',  # Correct collection name: 'playlist_follower'
            'localField': 'id',            # Local field in 'playlists' collection
            'foreignField': 'playlist_id',  # Corresponding field in 'playlist_follower' collection
            'as': 'followers'               # Output the joined data in a new field called 'followers'
        }
    })

    # Project the necessary fields for the response
    pipeline.append({
        '$project': {
            'playlist_name': 1,
            'owner_name': { '$arrayElemAt': [ '$owner.name', 0 ] },  # Extract owner name
            'created_date': 1,
            'followers_count': { '$size': '$followers' },  # Count the followers
            'songs': {
                '$map': {
                    'input': '$songs',  # Array of songs in 'songs' field
                    'as': 'song',
                    'in': {
                        'position': '$$song.position', 
                        'title': { '$arrayElemAt': [ '$song_details.title', 0 ] },  # Get song title
                        'artist': { '$arrayElemAt': [ '$song_details.artist', 0 ] }  # Get song artist
                    }
                }
            }
        }
    })

    
    # Fetch the query execution plan using explain()
    try:
        explain_result = explain_collection.aggregate(pipeline)
        print("EXPLAIN Results:")
        print(explain_result)  # Print the detailed execution plan to the console
    except Exception as e:
        print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")

    # Execute the aggregation pipeline
    playlists = list(playlists_collection.aggregate(pipeline))

    # Convert ObjectId to string for JSON serialization
    for playlist in playlists:
        playlist['_id'] = str(playlist['_id'])  # Convert ObjectId to string
        for follower in playlist.get('followers', []):
            follower['_id'] = str(follower['_id'])  # If you have follower _id, convert them

    if not playlists:
        return jsonify({'message': 'No playlists found with the given filters.'})

    return jsonify({'playlists': playlists})
    
# curl -Uri http://127.0.0.1:5000/playlists?name=newban -Method GET
# http://127.0.0.1:5000/playlists?name=newban
# endregion

# region Anforderung 4: Playlist hinzufügen

@PA3.route('/playlists/add', methods=['POST'])
def add_playlist():
    data = request.get_json()

    if not data or 'name' not in data or 'owner_id' not in data or 'songs' not in data:
        return jsonify({'message': 'Name, owner_id und songs sind erforderlich!'}), 400
    
    # Check if owner exists
    owner = users_collection.find_one({'id': data['owner_id']})
    if not owner:
        return jsonify({'message': 'Owner nicht gefunden!'}), 404
    
    # new playlist
    new_playlist = {
        "name": data['name'],
        "owner_id":data['owner_id'],
        "created_date": datetime.datetime.now() 
    }

    result = playlists_collection.insert_one(new_playlist)
    playlist_id = result.inserted_id

    # insert songs into playlist_song collection
    playlist_song = []
    for song_info in data['songs']:
        playlist_song.append({
            'playlist_id': playlist_id,
            'song_id': song_info['id'],
            'position': song_info['position']
        })
        
         # Überprüfung ob der Song existiert
        song = songs_collection.find_one({'id':song_info['id']})
        if not song:
            return jsonify({'message': 'Song nicht gefunden!'}), 404

    

    
    playlist_songs_collection.insert_many(playlist_song)

    return jsonify({'message': 'Playlist erfolgreich hinzugefügt!', 'id': str(playlist_id)}), 201

# endregion

# region get statistics

@PA3.route('/statistics', methods=['GET'])

def get_statistics():

    # Aggregation pipeline to get the statistics

    explain_collection = ExplainCollection(songs_collection)  # Wrap the collection with ExplainCollection

    try:
        # Aggregation pipeline to get the statistics
        pipeline = [
            {
                "$lookup": {
                    "from": "playlist_song",              # Collection to join with
                    "localField": "id",                  # Field in 'song' collection
                    "foreignField": "song_id",            # Field in 'playlist_song' collection
                    "as": "playlist_entries"              # Output array field
                }
            },
            {"$unwind": "$playlist_entries"},  # Flatten the playlist_entries array
            {
                "$group": {
                    "_id": "$artist",                           # Group by artist
                    "number_of_playlists": {"$sum": 1},         # Count total playlists the song appears in
                    "average_position": {"$avg": "$playlist_entries.position"},  # Calculate average position
                    "unique_songs": {"$addToSet": "$_id"}       # Collect unique song IDs
                }
            },
            {
                "$project": {
                    "_id": 0,                                   # Exclude default _id
                    "song_artist": "$_id",                      # Rename _id to song_artist
                    "number_of_playlists": 1,
                    "average_position": 1,
                    "unique_songs": {"$size": "$unique_songs"}  # Count unique songs per artist
                }
            },
            {
                "$sort": {"song_artist": 1}  # Sort alphabetically by song_artist (1 for ascending order)
            }
        ]

        # Fetch the query execution plan using explain()
        try:
            explain_result = explain_collection.aggregate(pipeline)  
            print("EXPLAIN Results:")
            print(explain_result)  # Print the detailed execution plan to the console
        except Exception as e:
            print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")
        
        # Execute the aggregation pipeline
        statistics = list(songs_collection.aggregate(pipeline))
        
        # Check if the statistics list is empty
        if not statistics:
            return jsonify({'message': 'No statistics found!'}), 404
        
        return jsonify({'statistics': statistics})
    
    except Exception as e:
        return jsonify({'message': 'Error fetching statistics!', 'error': str(e)}), 500


# endregion

if __name__ == '__main__':
    PA3.run(debug=True)
