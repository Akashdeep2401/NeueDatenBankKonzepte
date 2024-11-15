EXPLAIN
SELECT 
    p.name AS playlist_name,
    u.name AS owner_name,
    p.created_date,
    COUNT(DISTINCT pf.follower_id) AS followers_count,
    ps.position,
    s.title AS song_title,
    s.artist AS song_artist
FROM 
    playlist p
JOIN 
    user u ON p.owner_id = u.id
JOIN 
    playlist_song ps ON ps.playlist_id = p.id
JOIN 
    song s ON ps.song_id = s.id
LEFT JOIN -- instead of sub query to directly bring in followers
    playlist_follower pf ON pf.playlist_id = p.id
WHERE 
    p.name LIKE "newban"
GROUP BY 
    p.id, ps.position, s.id
ORDER BY 
    ps.position;
    
-- Index for <200 ms 
-- composite index to retrieve songs in the correct order
CREATE INDEX idx_playlist_song_playlist_id_position ON playlist_song(playlist_id, position);
-- to improve join of playlist table with the user table based on owner_id
CREATE INDEX idx_playlist_owner_id ON playlist(owner_id);

-- Drop Index
DROP INDEX idx_playlist_song_playlist_id_position ON playlist_song;
-- DROP INDEX idx_playlist_owner_id ON playlist; -> needed in foreign key constraint