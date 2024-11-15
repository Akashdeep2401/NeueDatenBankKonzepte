-- Wirefram Query
explain
SELECT 
    p.name AS playlist_name,
    u.name AS owner_name,
    p.created_date,
    -- Subquery to count distinct followers
    (SELECT COUNT(DISTINCT pf.follower_id) 
     FROM playlist_follower pf 
     WHERE pf.playlist_id = p.id) AS followers_count,
    ps.position,
    s.title AS song_title,
    s.artist AS song_artist
FROM 
    playlist p
JOIN 
    `user` u ON p.owner_id = u.id
JOIN 
    playlist_song ps ON ps.playlist_id = p.id
JOIN 
    song s ON ps.song_id = s.id
WHERE 
   p.name LIKE "newban"
ORDER BY 
    ps.position;


-- indexes for <200ms
CREATE INDEX idx_playlist_song_playlist_id ON playlist_song(playlist_id);
CREATE INDEX idx_playlist_owner_id ON playlist(owner_id);

-- Drop index
DROP INDEX  idx_playlist_song_playlist_id ON playlist_song;
DROP INDEX  idx_playlist_owner_id ON playlist;



-- ************************************************************************************************************************************************ --




-- Statistische Auswertung

EXPLAIN
SELECT 
    s.artist AS song_artist,
    SUM(ps.playlist_count) AS number_of_playlists,  -- Anzahl der Playlist-Einträge
    AVG(ps.position) AS average_position,              -- Durchschnittliche Position der Songs in den Playlisten
    COUNT(DISTINCT s.id) AS unique_songs              -- Anzahl unterschiedlicher Lieder in Playlisten pro Interpret
FROM 
    (
        -- Subquery -> Berechnung der Playlist-Einträge und durchschnittliche Position
        SELECT 
            song_id,
            COUNT(song_id) AS playlist_count,     -- Anzahl der Playlist-Einträge
            AVG(position) AS position            -- Durchschnittliche Position
        FROM 
            playlist_song
        GROUP BY 
            song_id
    ) ps -- Ergebnisse der Subquery werden als temporäre Tabelle playlist_song ps verwendet
JOIN 
    song s ON ps.song_id = s.id
GROUP BY 
    s.artist;
 
-- Index für <5 s

-- Beschleunigt Berechenung der durchschnittlichen Position in der Subquery 
CREATE INDEX idx_playlist_song_song_id_position_new ON playlist_song(song_id, position, id);
DROP INDEX idx_playlist_song_song_id_position_new ON playlist_song;


-- *************************************************************************************************************-- 

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















