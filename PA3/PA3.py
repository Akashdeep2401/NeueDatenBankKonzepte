from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from collections import defaultdict

PA3 = Flask(__name__)

# MongoDB Konfiguration
PA3.config["MONGO_URI"] = "mongodb://localhost:27017/Spotify"
mongo = PyMongo(PA3)

#region Anforderung 1:

# Lieder in einer paginierbaren und nach Titel und Interpret filterbaren Liste ausgeben
@PA3.route('/songs', methods=['GET'])
def get_songs():
    # Paginierung
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))
    
    # Filterung
    title_filter = request.args.get('title', '')
    artist_filter = request.args.get('artist', '')

    # Abfrage der Lieder mit Filterung
    query = {}
    if title_filter:
        query['title'] = {'$regex': title_filter, '$options': 'i'}  # Fallunempfindliche Suche
    if artist_filter:
        query['artist'] = {'$regex': artist_filter, '$options': 'i'}

    # Berechnung der Anzahl der Lieder
    total_songs = mongo.db.songs.count_documents(query)
    
    # Abrufen der Lieder mit Paginierung
    songs = mongo.db.songs.find(query).skip((page - 1) * page_size).limit(page_size)

    # Umwandeln der Lieder in eine Liste
    songs_list = []
    for song in songs:
        songs_list.append({
            'song_id': str(song['_id']),
            'title': song['title'],
            'artist': song['artist']
        })

    return jsonify({
        'total_songs': total_songs,
        'page': page,
        'page_size': page_size,
        'songs': songs_list
    })

# http://localhost:5000/songs?page=1&page_size=10
# http://localhost:5000/songs?page=1&page_size=10&title=Young Shock&artist=Federal Semih

#endregion

#region Anforderung 2: 

# Hinzufügen eines neuen Liedes
@PA3.route('/songs', methods=['POST'])
def add_song():

    data = request.get_json()
    if 'title' not in data or 'artist' not in data:
        return jsonify({'error': 'Title and artist are required'}), 400
    
    new_song = {
        'title': data['title'],
        'artist': data['artist']
    }

    result = mongo.db.songs.insert_one(new_song)
    return jsonify({'message': 'Song added', 'song_id': str(result.inserted_id)}), 201

# http://localhost:5000/songs"
# {
#     "title": "Mein neues Lied",
#     "artist": "Mein Künstler"
# }

# Ändern eines bestehenden Liedes
@PA3.route('/songs/<id>', methods=['PUT'])
def update_song(id):
    data = request.get_json()
    song = mongo.db.songs.find_one({'_id': ObjectId(id)})

    if not song:
        return jsonify({'error': 'Song not found'}), 404

    # Aktualisieren der Lieddaten
    updated_song = {}

    if 'title' in data:
        updated_song['title'] = data['title']

    if 'artist' in data:
        updated_song['artist'] = data['artist']

    mongo.db.songs.update_one({'_id': ObjectId(id)}, {'$set': updated_song})

    return jsonify({'message': 'Song updated'})

# http://localhost:5000/songs/<id>
# {
#     "title": "Mein aktualisiertes Lied"
# }


# Löschen eines Liedes
@PA3.route('/songs/<id>', methods=['DELETE'])
def delete_song(id):
    result = mongo.db.songs.delete_one({'_id': ObjectId(id)})

    if result.deleted_count == 0:
        return jsonify({'error': 'Song not found'}), 404

    return jsonify({'message': 'Song deleted'})

# http://localhost:5000/songs/<id>

#endregion

#region Anforderung 3: 

# Ausgabe einer Playlist inkl. User, Lieder und Follower
@PA3.route('/playlists/<id>', methods=['GET'])
def get_playlist(id):
    # Suche nach der Playlist in der Datenbank
    playlist = mongo.db.playlists.find_one({'_id': ObjectId(id)})

    if not playlist:
        return jsonify({'error': 'Playlist not found'}), 404

    # Besitzer der Playlist holen
    owner = mongo.db.users.find_one({'_id': ObjectId(playlist['owner_id'])})
    owner_name = owner['name'] if owner else "Unknown User"

    # Lieder der Playlist holen
    song_ids = [song['song_id'] for song in playlist['songs']]
    songs = []
    for song_id in song_ids:
        song = mongo.db.songs.find_one({'_id': ObjectId(song_id)})
        if song:
            songs.append({
                'song_id': str(song['_id']),
                'title': song['title'],
                'artist': song['artist'],
                'position': next((s['position'] for s in playlist['songs'] if s['song_id'] == song_id), None)
            })

    # Follower holen
    followers = []
    for follower_id in playlist['followers']:
        follower = mongo.db.users.find_one({'_id': ObjectId(follower_id)})
        if follower:
            followers.append({
                'follower_id': str(follower['_id']),
                'name': follower['name']
            })

    # Umwandeln der Playlist in ein lesbares Format
    playlist_data = {
        'playlist_id': str(playlist['_id']),
        'name': playlist['name'],
        'created_date': playlist['created_date'],
        'owner': {
            'owner_id': str(playlist['owner_id']),
            'name': owner_name
        },
        'followers': followers,
        'songs': songs
    }

    return jsonify(playlist_data)

# http://localhost:5000/playlists/<id>

#endregion

#region Anforderung 4:

# Hinzufügen einer Playlist inkl. Lieder und Verweis auf einen bereits existierenden User
@PA3.route('/playlists', methods=['POST'])
def create_playlist():
    data = request.get_json()
    
    # Überprüfen, ob die erforderlichen Felder vorhanden sind
    if 'name' not in data or 'owner_id' not in data or 'songs' not in data:
        return jsonify({'error': 'Name, owner_id, and songs are required'}), 400
    
    # Überprüfen, ob der Benutzer existiert
    owner = mongo.db.users.find_one({'_id': ObjectId(data['owner_id'])})
    if not owner:
        return jsonify({'error': 'Owner user not found'}), 404

    # Playlist-Daten erstellen
    new_playlist = {
        'name': data['name'],
        'created_date': data.get('created_date', '2023-01-01'),
        'owner_id': ObjectId(data['owner_id']),
        'followers': [],
        'songs': []
    }

    # Füge die Lieder zur Playlist hinzu
    for song in data['songs']:
        if 'song_id' in song and 'position' in song:
            new_playlist['songs'].append({
                'song_id': ObjectId(song['song_id']),
                'position': song['position']
            })
        else:
            return jsonify({'error': 'Each song must have song_id and position'}), 400

    # Füge die neue Playlist zur Datenbank hinzu
    result = mongo.db.playlists.insert_one(new_playlist)

    return jsonify({'message': 'Playlist created', 'playlist_id': str(result.inserted_id)}), 201

# http://localhost:5000/playlists
# {
#     "name": "Meine neue Playlist",
#     "owner_id": "id_user",  // Beispiel-ID eines existierenden Benutzers
#     "songs": [
#         {
#             "song_id": "id_lied1",  // Beispiel-ID eines existierenden Liedes
#             "position": 1
#         },
#         {
#             "song_id": "id_lied2",  // Beispiel-ID eines weiteren existierenden Liedes
#             "position": 2
#         }
#     ]
# }

#endregion

#region Statistische Auswertung:

# Anzahl der Playlist-Einträge, durchschnittliche Playlist-Position und Anzahl der unterschiedlichen Lieder auf Playlisten je Interpret.
@PA3.route('/statistics', methods=['GET'])
def get_statistics():
    pipeline = [
        # Unwind die Songs, um sie einzeln zu verarbeiten
        {
            '$unwind': '$songs'
        },
        # Lookup, um die Songdetails (Titel und Interpret) zu erhalten
        {
            '$lookup': {
                'from': 'songs',
                'localField': 'songs.song_id',
                'foreignField': '_id',
                'as': 'song_details'
            }
        },
        # Unwind die Songdetails, um sie einzeln zu verarbeiten
        {
            '$unwind': '$song_details'
        },
        # Groupiere die Daten nach dem Interpreten
        {
            '$group': {
                '_id': '$song_details.artist',
                'number_of_playlists': {'$sum': 1},  # Anzahl der Playlists
                'average_position': {'$avg': '$songs.position'},  # Durchschnittliche Position
                'unique_songs': {'$addToSet': '$song_details._id'}  # Einzigartige Lieder
            }
        },
        # Berechne die Anzahl der einzigartigen Lieder
        {
            '$project': {
                'artist': '$_id',
                'number_of_playlists': 1,
                'average_position': 1,
                'unique_song_count': {'$size': '$unique_songs'}  # Anzahl der einzigartigen Songs
            }
        }
    ]

    # Aggregation ausführen
    statistics = list(mongo.db.playlists.aggregate(pipeline))
    # explain_result = mongo.db.playlists.aggregate(pipeline, explain=True)

    # Überprüfung, ob die Ergebnisse leer sind
    if not statistics:
        return jsonify({'message': 'No statistics found!'}), 404

    return jsonify({'statistics': statistics})

# http://localhost:5000/statistics


#endregion

if __name__ == '__main__':
    PA3.run(debug=True)