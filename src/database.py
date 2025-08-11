import sqlite3

def get_con(db_path:str="data/activities/strava.db"):
    con = sqlite3.connect(db_path)
    try:
        yield con
    finally:
        con.commit()
        con.close()

def init_db(db_path:str="data/activities/strava.db"):
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute(
        """
        CREATE TABLE IF NOT EXISTS activities (
            id INTIGER PRIMARY KEY,
            name TEXT,
            start_date TEXT,
            moving_time REAL,
            elapsed_time REAL,
            distance REAL,
            total_elevation_gain REAL,
            average_speed REAL,
            max_speed REAL,
            average_cadence REAL,
            average_watts REAL,
            max_watts REAL,
            average_heartrate REAL,
            max_heartrate REAL,
            elev_high REAL,
            elev_low REAL,
            type TEXT,
            has_stream INTIGER DEFAULT 0
        )
        """
        )
        c.execute(
        """
        CREATE TABLE IF NOT EXISTS streams (
            activity_id INTIGER PRIMARY KEY,
            distance REAL,
            altitude REAL,
            velocity REAL,
            heartrate REAL,
            cadence REAL,
            grade REAL,
            FOREIGN KEY(activity_id) REFERENCES activities(id)
        )
        """
        )

def insert_activities(activities,db_path:str="data/activities/strava.db"):
    with get_con(db_path) as conn:
        c = conn.cursor()
        for act in activities:
            c.execute(
            """
            INSERT OR IGNORE INTO activities (id, name, start_date, moving_time, elapsed_time, distance, total_elevation_gain,
            average_speed, max_speed, average_cadence, average_watts, max_watts, average_heartrate, max_heartrate, elev_high, elev_low, 
            type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (act["id"],act["name"],act["start_date"], act["moving_time"], act["elapsed_time"], act["distance"], act["total_elevation_gain"],
             act["average_speed"], act["max_speed"], act["average_cadence"], act["average_watts"], act["max_watts"], act["average_heartrate"],
             act["max_heartrate"], act["elev_high"], act["elev_low"], act["type"])
            )
    