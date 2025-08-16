import sqlite3
import pathlib
from contextlib import contextmanager


@contextmanager
def get_con(db_path:str="data/activities/strava.db"):
    con = sqlite3.connect(db_path)
    try:
        yield con
    finally:
        con.commit()
        con.close()

def query_db(query:str, params:tuple = (), db_path:str="data/activities/strava.db", as_dict:bool = True):
    if not pathlib.Path(db_path).is_file():
        return None 
    try:
        with get_con(db_path) as conn:
            conn.row_factory = sqlite3.Row if as_dict else None
            c = conn.cursor()
            c.execute(query,params)
            rows = c.fetchall()

            if as_dict: 
                return [dict(row) for row in rows]
            return rows
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return None 
        else:
            raise

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
    if not pathlib.Path(db_path).is_file():
        return None 
    with get_con(db_path) as conn:
        c = conn.cursor()
        for act in activities:
            c.execute(
            """
            INSERT OR IGNORE INTO activities (id, name, start_date, moving_time, elapsed_time, distance, total_elevation_gain,
            average_speed, max_speed, average_cadence, average_watts, max_watts, average_heartrate, max_heartrate, elev_high, elev_low, 
            type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                act.get("id"),
                act.get("name"),
                act.get("start_date"),
                act.get("moving_time"),
                act.get("elapsed_time"),
                act.get("distance"),
                act.get("total_elevation_gain"),
                act.get("average_speed"),
                act.get("max_speed"),
                act.get("average_cadence"),
                act.get("average_watts"),
                act.get("max_watts"),
                act.get("average_heartrate"),
                act.get("max_heartrate"),
                act.get("elev_high"),
                act.get("elev_low"),
                act.get("type"),
            ))