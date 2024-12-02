## Datenstruktur MongoDB

### Datenmodell

* Benutzer: Ein Dokument pro Benutzer mit grundlegenden Informationen
* Lieder: Ein Dokumet pro Lied mit Titel und Interpret
* Playlists: Ein Dokument pro Playlist, das den Benutzer, die Lieder und die Follower enthält

### User Collection:

```json
{
    "_id": ObjectId,
    "name": "Benutzername",
    "gender": "m/w/d"
}
```
### Songs Collection:

```json
{
    "_id": ObjectId,
    "title": "Liedtitel",
    "artist": "Interpret",
}
```
### Playlists Collection:

```json
{
    "_id": ObjectId,
    "name": "Playlistname",
    "created_date": "Erstellungsdatum",
    "owner_id": ObjectId,  // Referenz auf User
    "followers": [ObjectId],  // Liste von Follower-IDs
    "songs": [
        {
            "song_id": ObjectId,
            "position": 1  // Position des Liedes in der Playlist
        }
    ]
}
```

