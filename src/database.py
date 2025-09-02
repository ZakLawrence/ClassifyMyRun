import sqlite3
import pathlib
import json
import requests
import datetime
from contextlib import contextmanager


@contextmanager
def get_con(db_path:str="data/activities/strava.db"):
    con = sqlite3.connect(db_path)
    try:
        yield con
    finally:
        con.commit()
        con.close()

def query_db(query:str, params:tuple = (), db_path:str="data/activities/strava.db", as_dict:bool = True) -> dict | None:
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

def init_db(db_path:str="data/activities/strava.db",table:str="") -> None:
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute(table)

def insert_activities(activities,db_path:str="data/activities/strava.db") -> None:
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

def insert_stream(activity_id:int, activity_stream:dict, db_path:str="data/activities/strava.db") -> None:
    """
    Insert streams for a give activity into the database.
    
    """
    if not pathlib.Path(db_path).is_file():
        raise FileNotFoundError(f"No database found at {db_path}")

    with get_con(db_path) as conn:
        c = conn.cursor()

        for stream_type, stream_obj in activity_stream.items():
            data = stream_obj.get("data",[])
            if not data:
                continue

            c.execute(
                """
                INSERT OR IGNORE INTO streams (activity_id, stream_type, data)
                VALUES (?, ?, ?)
                """,
                (activity_id,stream_type,json.dumps(data))
            )
            c.execute("UPDATE activities SET has_stream = 1 WHERE id = ?",(activity_id,))  

def log_requests(request:requests.models.Response,db_path:str="logs/requests.db") -> None:
    if not pathlib.Path(db_path).is_file():
        raise FileNotFoundError(f"No database found at {db_path}")
    
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO requests (timestamp,headers) VALUES (?, ?)",
            (datetime.datetime.now().isoformat(), json.dumps(dict(request.headers)))
        )

def get_requests_today(db_path:str) -> list[dict]:
    today = datetime.date.today().isoformat()
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT timestamp, headers FROM requests WHERE DATE(timestamp) = ?",
            (today,)
        )
        rows = c.fetchall()
        return [{"timestamp":ts, "headers":json.loads(h)} for ts,h in rows]
    
def job_ran_today(job_name: str, db_path: str) -> bool:
    today = datetime.date.today().isoformat()
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute("SELECT last_run FROM jobs WHERE name = ?", (job_name,))
        row = c.fetchone()
        return row is not None and row[0].startswith(today)

def mark_job_ran(job_name: str, db_path: str) -> None:
    today = datetime.datetime.now().isoformat()
    with get_con(db_path) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO jobs (name, last_run) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET last_run = excluded.last_run",
            (job_name, today)
        )
