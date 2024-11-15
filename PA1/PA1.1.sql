-- Statistische Auswertung

-- Ohne Subquery
explain
SELECT 
    s.artist AS song_artist,
    COUNT(ps.id) AS number_of_playlists,  -- Anzahl der Playlist-Einträge
    AVG(ps.position) AS average_position,  -- Durchschnittliche Position der Songs in den Playlisten
    COUNT(DISTINCT s.id) AS unique_songs  -- Anzahl unterschiedlicher Lieder in Playlisten pro Interpret
FROM 
    playlist_song ps
JOIN 
    song s ON ps.song_id = s.id
GROUP BY 
    s.artist;
    
    
-- Beschleunigt Berechenung der durchschnittlichen Position in der Subquery 
CREATE INDEX idx_playlist_song_song_id_position_new ON playlist_song(song_id, position, id);
DROP INDEX idx_playlist_song_song_id_position_new ON playlist_song;