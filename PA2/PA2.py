from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from graphene import ObjectType, String, List, Schema
from graphene_sqlalchemy import SQLAlchemyObjectType
from flask_graphql import GraphQLView

# Erstelle eine Instanz der Flask-Anwendung
PA_2 = Flask(__name__)

# Konfiguration der MySQL-Datenbankverbindung
PA_2.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:Parkash%402001@localhost:3306/scenario_spotify'
PA_2.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialisiere SQLAlchemy
db = SQLAlchemy(PA_2)

@PA_2.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "running"})


@PA_2.route('/test_db_connection', methods=['GET'])
def test_db_connection():
    try:
        # Führe eine einfache Abfrage durch
        result = db.session.execute(text('SELECT 1'))  # Eine einfache Abfrage, um die Verbindung zu testen
        return jsonify({'message': 'Datenbankverbindung erfolgreich!', 'result': [row[0] for row in result]})
    except Exception as e:
        return jsonify({'message': 'Fehler bei der Datenbankverbindung:', 'error': str(e)})

#region SQL Alchemy Model

class Song(db.Model):
    __tablename__ = 'song'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    artist = db.Column(db.String(100), nullable=False)

class Playlist(db.Model):
    __tablename__ = 'playlist'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_date = db.Column(db.DateTime)

class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

class PlaylistSong(db.Model):
    __tablename__ = 'playlist_song'
    playlist_id = db.Column(db.Integer, db.ForeignKey('playlist.id'), primary_key=True)
    song_id = db.Column(db.Integer, db.ForeignKey('song.id'), primary_key=True)
    position = db.Column(db.Integer)

class PlaylistFollower(db.Model):
    __tablename__ = 'playlist_follower'
    playlist_id = db.Column(db.Integer, db.ForeignKey('playlist.id'), primary_key=True)
    follower_id = db.Column(db.Integer, db.ForeignKey('user.id'), primary_key=True)

#endregion

#region Routes 

# Flask-Dekorator, der neue Route für die Anwendung erstellt
@PA_2.route('/routes', methods=['GET'])
def get_routes():

    # Zur Speicherung der Routen
    routes = []

    # Durch alle Routen iterieren, die in URL-Karte von Flask definiert sind
    for rule in PA_2.url_map.iter_rules():

        # Jede Route als String speichern
        routes.append(str(rule)) 
    print("Current routes:", routes) 

    # Flask-Funktion zur Sicherstellung des richtigen JSON-Formats
    return jsonify(routes)

#endregion

# region Anforderung 1: Ausgabe

@PA_2.route('/songs', methods=['GET'])
def get_songs():
    # Filter-Parameter aus der Anfrage abrufen
    title_filter = request.args.get('title')
    artist_filter = request.args.get('artist')

    # Paginierungs-Parameter
    page = request.args.get('page', 1, type=int)  # Standard auf Seite 1
    per_page = request.args.get('per_page', 10, type=int)  # 10 Lieder pro Seite

    # Grundabfrage
    query = Song.query

    # Filter anwenden (falls vorhanden)
    if title_filter:
        query = query.filter(Song.title.ilike(f"%{title_filter}%"))
    if artist_filter:
        query = query.filter(Song.artist.ilike(f"%{artist_filter}%"))

    # -- Überprüfung auf Index-Nutzung mit EXPLAIN (ID, Select Type, Table, Type, Possible Keys, Key, Key Length, Rows, Extra)

    # SQL-Text der Abfrage generieren
    sql_query = str(query.statement)

    # EXPLAIN-Befehl hinzufügen
    explain_query = f"EXPLAIN {sql_query}"

    # Parameter für die EXPLAIN-Abfrage setzen
    parameters = {}
    if title_filter:
        parameters['title_1'] = f"%{title_filter}%"
    if artist_filter:
        parameters['artist_1'] = f"%{artist_filter}%"

    # Ausführen der EXPLAIN-Abfrage
    try:
        explain_result = db.session.execute(text(explain_query), parameters)
        explain_results = explain_result.fetchall()

        # Überprüfen, ob explain_results leer ist
        if not explain_results:
            print("Keine Ergebnisse für EXPLAIN gefunden.")

        # Ergebnisse des EXPLAIN-Befehls in der Konsole ausgeben
        print("EXPLAIN Results:")

        for row in explain_results:
            print(row)

    except Exception as e:
        print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")
    
    # -- Ende Überprüfung Index --

    # Paginierung anwenden
    paginated_songs = query.paginate(page=page, per_page=per_page, error_out=False)

     # Prüfen, ob Songs vorhanden sind
    if not paginated_songs.items:
        return jsonify({
            'message': 'Kein Song gefunden mit den gegebenen Filtern.'
        })

    # Lieder in JSON-Format zurückgeben
    songs_list = [{'id': song.id, 'title': song.title, 'artist': song.artist} for song in paginated_songs.items]
    
    return jsonify({
        'songs': songs_list,
        'total': paginated_songs.total,
        'pages': paginated_songs.pages,
        'current_page': paginated_songs.page
    })

# Alle Songs:
# http://127.0.0.1:5000/songs
# Nach Song-Titel sortiert (vice versa für Artist):
# http://127.0.0.1:5000/songs?title=love&page=1&per_page=5
# Song konnte nicht gefunden werden:
# http://127.0.0.1:5000/songs?artist=shreya&page=1&per_page=5

# endregion


# region Anforderung 2: Song hinzufügen

@PA_2.route('/songs/add', methods=['POST'])  
def add_song():
    # Daten der Anfrage holen
    data = request.get_json()

    # Check ob die benötigten Felder vorhanden sind
    if not data or 'title' not in data or 'artist' not in data:
        return jsonify({'message': 'Titel und Künstler sind erforderlich!'}), 400

    # Neues Song Objekt erstellene
    new_song = Song(title=data['title'], artist=data['artist'])

    # Song zur Datenbank hinzufügen
    try:
        db.session.add(new_song)
        db.session.commit()
        return jsonify({'message': 'Lied erfolgreich hinzugefügt!', 'song_id': new_song.id}), 201
    except Exception as e:
        db.session.rollback()  # Rollback der Transaktion im Fehlerfall
        return jsonify({'message': 'Fehler beim Hinzufügen des Liedes:', 'error': str(e)}), 500

# curl -Uri http://127.0.0.1:5000/songs/add -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"title": "New Song Title", "artist": "New Artist"}'
# http://127.0.0.1:5000/songs?title=New Song Title new new&page=1 


#endregion


#region Anforderung 2: Song verändern

@PA_2.route('/songs/<int:song_id>', methods=['PUT'])
def change_song(song_id):
    print(f"Received request to change song with ID: {song_id}")
    
    data = request.get_json()
    print(f"Request data: {data}")
    
    if not data or ('title' not in data and 'artist' not in data):
        return jsonify({'message': 'Title or artist is required!'}), 400

    song = db.session.get(Song, song_id)
    if not song:
        return jsonify({'message': 'Song not found!'}), 404

    # Titel oder Artist verändern
    if 'title' in data:
        song.title = data['title']
    if 'artist' in data:
        song.artist = data['artist']

    # Anpassung in der Datenbank hinzufügen
    try:
        db.session.commit()
        return jsonify({'message': 'Song successfully updated!', 'song_id': song.id}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': 'Error updating the song:', 'error': str(e)}), 500

# curl -Uri http://127.0.0.1:5000/songs/8001 -Method Put -Headers @{"Content-Type"="application/json"} -Body '{"title": "Updated Song Title", "artist": "Updated Artist"}'


#endregion


#region Anforderung 2: Song löschen

@PA_2.route('/songs/delete/<int:song_id>', methods=['DELETE'])
def delete_song(song_id):
    print(f"Received request to delete song with ID: {song_id}")
    
    # Song mit ID finden
    song = db.session.get(Song, song_id)

    # Wenn der Song nicht exisitert, 404 zurückgeben
    if not song:
        return jsonify({'message': 'Song not found!'}), 404

    # Den Song in der Datenbank löschen
    try:
        db.session.delete(song)
        db.session.commit()
        return jsonify({'message': 'Song successfully deleted!', 'song_id': song.id}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': 'Error deleting the song:', 'error': str(e)}), 500

# curl -Uri http://127.0.0.1:5000/songs/delete/8001 -Method DELETE        


#endregion


# region GraphQL schema

class UserType(SQLAlchemyObjectType):
    class Meta:
        model = User

class SongType(SQLAlchemyObjectType):
    class Meta:
        model = Song
    
class PlaylistType(SQLAlchemyObjectType):
    class Meta:
        model = Playlist
    
    songs = List(lambda: SongType)

    def resolve_songs(parent, info):
        return db.session.query(Song).\
            join(PlaylistSong, Song.id == PlaylistSong.song_id).\
            filter(PlaylistSong.playlist_id == parent.id).all()

class Query(ObjectType):
   playlists = List(PlaylistType, name = String(required = True))

   def resolve_playlists(root, info, name):
        query = Playlist.query
        if name:
            query = query.filter(Playlist.name.ilike(f"%{name}%"))
        return query.all()

schema = Schema(query=Query)

# endregion

# region Anforderung 3: Ausgabe Playlist inkl. User, Lieder und Followern

@PA_2.route('/playlists', methods=['GET'])
def graphql_playlists():
    name_filter = request.args.get('name')

    # Grundabfrage
    query = db.session.query(
        Playlist, User, Song, PlaylistSong,
        db.func.count(PlaylistFollower.follower_id).label('followers_count')
    ).join(User, Playlist.owner_id == User.id)\
     .join(PlaylistSong, Playlist.id == PlaylistSong.playlist_id)\
     .join(Song, PlaylistSong.song_id == Song.id)\
     .outerjoin(PlaylistFollower, Playlist.id == PlaylistFollower.playlist_id)\
     .group_by(Playlist.id, User.id, Song.id, PlaylistSong.position)

    # -- Überprüfung auf Index-Nutzung mit EXPLAIN (ID, Select Type, Table, Type, Possible Keys, Key, Key Length, Rows, Extra)

    # SQL-Text der Abfrage generieren
    sql_query = str(query.statement)

    # Ersetzen von doppelten Anführungszeichen durch Backticks
    sql_query = sql_query.replace('"', '`')

    # EXPLAIN-Befehl hinzufügen
    explain_query = f"EXPLAIN {sql_query}"

    # Ausführen der EXPLAIN-Abfrage ohne Filter
    try:
        explain_result = db.session.execute(text(explain_query))
        explain_results = explain_result.fetchall()

        # Ergebnisse des EXPLAIN-Befehls in der Konsole ausgeben
        print("EXPLAIN Results:")
        for row in explain_results:
            print(row)

    except Exception as e:
        print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")
    
    # -- Ende Überprüfung Index --

    # Filter der Abfrage hinzufügen, wenn vorhanden
    if name_filter:
        query = query.filter(Playlist.name.ilike(f"%{name_filter}%"))

    # Ausführen der ursprünglichen Abfrage
    result = query.all()

    playlists = []

    # Iteration durch die ergebnisse für playlist daten
    for playlist, user, song, playlist_song, followers_count in result:
        # Überprüfung, ob die Playlist bereits in playlists existiert
        playlist_data = next((p for p in playlists if p['playlist_name'] == playlist.name), None)

        if not playlist_data:
            # Wenn nicht neue Erstellung
            playlist_data = {
                'playlist_name': playlist.name,
                'owner_name': user.name,
                'created_date': playlist.created_date.strftime('%Y-%m-%d'),
                'followers_count': followers_count,
                'songs': []
            }
            playlists.append(playlist_data)

        # Song Details zur Song-List der Playlist anhängen
        playlist_data['songs'].append({
            'position': playlist_song.position,
            'title': song.title,
            'artist': song.artist
        })

    # Einrückung fürs return Statement
    return jsonify({'playlists': playlists})

# curl -Uri http://127.0.0.1:5000/playlists?name=newban -Method GET
# http://127.0.0.1:5000/playlists?name=newban

#GraphQL Endpoint

PA_2.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

# curl -Uri http://127.0.0.1:5000/graphql -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"query": "{ playlists(name: \"newban\") { name ownerId songs {title artist} } }"}
""" {
  playlists(name: "newban") {
    name
    ownerId
    songs {
      title
      artist
    }
  }
} """

# endregion

# region Anforderung 4: Hinzufügen einer Playlist inkl. Lieder & Verweis auf existierenden User

@PA_2.route('/playlists/add', methods=['POST'])
def add_playlist():
    data = request.get_json()

    # Überprüfung ob die Felder vorhanden sind
    if not data or 'name' not in data or 'owner_id' not in data or 'songs' not in data:
        return jsonify({'message': 'Name, owner_id, and songs are required!'}), 400

    # Überprüfung ob der User existiert
    owner = db.session.get(User, data['owner_id'])
    if not owner:
        return jsonify({'message': 'Owner not found!'}), 404

    # Neues Playlist-Objekt erstellen
    new_playlist = Playlist(name=data['name'], owner_id=data['owner_id'], created_date=db.func.now())   

    # Playlist zur Datenbank hinzufügen
    try:
        db.session.add(new_playlist)
        db.session.commit()  # Commit für die playlist ID

        # Songs zur Playlist hinzufügen
        for song_info in data['songs']:
            song_id = song_info['id']  # Get the song id from the current song info
            position = song_info.get('position')  # Get the position of the song

            # Überprüfung ob der Song existiert
            song = db.session.get(Song, song_id)
            if not song:
                return jsonify({'message': f'Song with ID {song_id} not found!'}), 404

            # Erstellung und Hinzufügen des neuen Playlist Objekts
            playlist_song = PlaylistSong(playlist_id=new_playlist.id, song_id=song_id, position=position)
            db.session.add(playlist_song)

        db.session.commit()  # Commit zum hinzufügen der songs in der playlist
        return jsonify({'message': 'Playlist successfully added!', 'playlist_id': new_playlist.id}), 201 
    
    except Exception as e:
        db.session.rollback()  # Rollback falls ein Fehler auftritt
        return jsonify({'message': 'Error adding the playlist:', 'error': str(e)}), 500

# curl -Uri http://127.0.0.1:5000/playlists/add -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"name": "My new Playlist", "owner_id": 1, "songs": [{"id": 1, "position": 1}, {"id": 2, "position": 2}, {"id": 3, "position": 3}]}'

#endregion

#region Statische Auswertung

@PA_2.route('/statistics', methods=['GET'])
def get_statistics():
    # Subquery für Playlist-Einträge und durchschnittliche Position
    subquery = db.session.query(
        PlaylistSong.song_id.label('song_id'),
        db.func.count(PlaylistSong.song_id).label('playlist_count'),
        db.func.avg(PlaylistSong.position).label('avg_position')
    ).group_by(PlaylistSong.song_id).subquery()

    # Hauptanfrage für die Statistiken
    query = db.session.query(
        Song.artist.label('song_artist'),
        db.func.sum(subquery.c.playlist_count).label('number_of_playlists'),  # Anzahl der Playlist-Einträge
        db.func.avg(subquery.c.avg_position).label('average_position'),       # Durchschnittliche Position
        db.func.count(db.func.distinct(Song.id)).label('unique_songs')        # Anzahl unterschiedlicher Lieder in Playlists pro Interpret
    ).join(subquery, Song.id == subquery.c.song_id).group_by(Song.artist)

    # -- Überprüfung auf Index-Nutzung mit EXPLAIN (ID, Select Type, Table, Type, Possible Keys, Key, Key Length, Rows, Extra)

    # SQL-Text der Abfrage generieren
    sql_query = str(query.statement)

    # Ersetzen von doppelten Anführungszeichen durch Backticks
    sql_query = sql_query.replace('"', '`')

    # EXPLAIN-Befehl hinzufügen
    explain_query = f"EXPLAIN {sql_query}"

    # Ausführen der EXPLAIN-Abfrage ohne Filter
    try:
        explain_result = db.session.execute(text(explain_query))
        explain_results = explain_result.fetchall()

        # Ergebnisse des EXPLAIN-Befehls in der Konsole ausgeben
        print("EXPLAIN Results:")
        for row in explain_results:
            print(row)

    except Exception as e:
        print(f"Fehler beim Ausführen der EXPLAIN-Abfrage: {e}")

    # -- Ende Überprüfung Index --

    # Ausführen der ursprünglichen Abfrage und Abrufen der Ergebnisse
    final_result = query.all() 

    # Überprüfung ob die Ergebnisse leer sind
    if not final_result:
        return jsonify({'message': 'No statistics found!'}), 404
    
    # Ausgabe richtig formatieren
    statistics = []
    for row in final_result:
        statistics.append({
            'song_artist': row.song_artist,
            'number_of_playlists': row.number_of_playlists,
            'average_position': row.average_position,
            'unique_songs': row.unique_songs
        })
    
    return jsonify({'statistics': statistics})

# curl -Uri http://127.0.0.1:5000/statistics -Method GET
# http://127.0.0.1:5000/statistics

#endregion

#start the flask application
if __name__ == '__main__':
    PA_2.run(debug=True)

