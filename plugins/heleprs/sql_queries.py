
class SqlQueries:
    songplay_table_insert = "SELECT * FROM staging_events"
    user_table_insert = "SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events"
    song_table_insert = "SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs"
    artist_table_insert = "SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs"
    time_table_insert = "SELECT DISTINCT ts, EXTRACT(hour FROM ts), EXTRACT(day FROM ts), EXTRACT(week FROM ts), EXTRACT(month FROM ts), EXTRACT(year FROM ts), EXTRACT(dow FROM ts) FROM staging_events"
