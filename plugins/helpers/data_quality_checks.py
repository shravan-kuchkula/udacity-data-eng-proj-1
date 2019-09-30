class DataQualityChecks:
    # Check if all artists have names
    artists_table_check = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE name IS NULL;
    """)

    # Check if all the rows are from 2018 only
    songplays_table_check = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE extract(year from start_time) != 2018;
    """
    )

    # Check whether the duration is range
    songs_table_check = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE duration NOT BETWEEN 1 and 3000;
    """
    )

    # Check whether all the rows are before week 44
    time_table_check = ("""
        SELECT COUNT(*)
        FROM time
        WHERE week < 44 and year = 2018;
    """
    )

    # Check whether level and gender have correct values
    users_table_check = ("""
        SELECT COUNT(*)
        FROM users
        WHERE level NOT IN ('free', 'paid')
        AND gender NOT IN ('F', 'M');
    """
    )

    def get_data_quality_checks():
        checks = {
            "artists": DataQualityChecks.artists_table_check,
            "songs": DataQualityChecks.songs_table_check,
            "time": DataQualityChecks.time_table_check,
            "users": DataQualityChecks.users_table_check,
            "songplays": DataQualityChecks.songplays_table_check
            }
        return checks
