import api_tools.strava_api as strava_api
import database
import logging
import traceback
from utils import RateLimitExceeded, chunk_list
from pathlib import Path
from config import load_config, save_config, CONFIG_FILE, Config

logging.basicConfig(
    filename="logs/pipeline.log",  # you can make this cfg.log_dir / "pipeline.log"
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DB_FILE: Path | None = None
LOG_FILE: Path | None = None

def check_table_size(database_file:Path, table:str) -> int:
    db_size = database.query_db(f"SELECT COUNT(*) FROM {table}",db_path=database_file)
    return db_size[0]["COUNT(*)"]

def ids_in_db(ids:list[int],db_file:str, table:str="activities",
              id_col_name:str="id") -> list[int]:
    if not ids:
        return []
    placeholder = ",".join("?" for _ in ids)
    query = f"SELECT {id_col_name} FROM {table} WHERE {id_col_name} IN ({placeholder})"

    existing_ids = database.query_db(query,ids,db_path=db_file)
    existing_ids = [row["id"] for row in existing_ids]
    
    return [i for i in ids if i not in existing_ids]

def initilise_database(cfg:Config) -> None:
    database_file = DB_FILE
    if not database_file.exists():
        database.init_db(db_path=database_file,
                         table= 
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
            """)
        database.init_db(db_path=database_file,
                         table= 
            """
            CREATE TABLE IF NOT EXISTS streams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER,
                stream_type TEXT,
                data TEXT,
                UNIQUE(activity_id, stream_type)
            )
            """
        )
    log_database_file = Path(cfg.log_dir)/ "pipeline.db"
    if not log_database_file.exists():
        database.init_db(db_path=log_database_file,
                        table=
                        """
                        CREATE TABLE IF NOT EXISTS jobs (
                            name TEXT PRIMARY KEY,
                            last_run TEXT NOT NULL
                        )
                        """ 
        )

def fetch_all_activities(cfg:Config,force:bool=False) -> None:
    if database.job_ran_today("fetch_all_activities",LOG_FILE) and not force:
        logging.info("fetch_all_activities already completed, skipping")
        return
    
    logging.info("Running fetch_all_activities.")
    database_file = DB_FILE
    if not database_file.exists():
        logging.error(f"fetch_all_activities: Failed to find database at location: {database_file}")
        raise RuntimeError(f"Initilise step failed to create datbase! Target file: {database_file}")
    if check_table_size(database_file,"activities") > 0 and not force:
        logging.info(f"fetch_all_activities: Database exists skipping")
        database.mark_job_ran("fetch_all_activities",LOG_FILE)
        return

    page = 1
    total_inserted = 0 
    while True:
        logging.info(f"fetch_all_activities: Getting activities on page {page}")
        args = {
            "page": page,
            "per_page": cfg.per_page
        }
        try:
            activities, resp = strava_api.get_activities(**args)
        except RateLimitExceeded as e:
            logging.warning(f"Rate limit exceeded on page {page}: {e}")
            marker_file = Path(cfg.data_dir) / "resume_marker.txt"
            marker_file.write_text(str(page))
            logging.info(f"Saved resume marker at page {page}. Exiting gracefully.")
            raise e
        except Exception as e:
            logging.error(f"Error fetching activities on page {page}: {e}")
            logging.error(traceback.format_exc())
            raise e

        if not activities:
            logging.info(f"fetch_all_activities: No activities found on page {page}. Exiting function")
            break
        run_activities = [
            a for a in activities 
            if a.get("type") == "Run"
            and not a.get("trainer",False)
        ]
        if not run_activities:
            logging.info(f"fetch_all_activities: No valid runs of page {page}. Exiting function")
            break
        database.insert_activities(run_activities,database_file)
        total_inserted += len(run_activities)
        page +=1
    logging.info(f"Finished fetch_all_activities. Total inserted: {total_inserted}")
    database.mark_job_ran("fetch_all_activities",LOG_FILE)


def update_database(cfg:Config,force:bool=False) -> None:
    if database.job_ran_today("update_database",LOG_FILE) and not force:
        logging.info("update_database already completed, skipping")
        return 

    logging.info("Running update_database.")
    database_file = DB_FILE
    if not database_file.exists():
        logging.error(f"update_database: Failed to find database at location: {database_file}")
        raise RuntimeError(f"Initilise step failed to create datbase! Target file: {database_file}")
    latest_activity = database.query_db(
        "SELECT MAX(start_date) as last_date FROM activities",
        db_path=database_file
        )[0]['last_date']
    
    page = 1
    total_inserted = 0 
    while True:
        logging.info(f"update_database: Getting activities on page {page}")
        args = {
            "page": page, 
            "per_page": cfg.per_page
        }
        try:
            activities, resp = strava_api.get_activities(**args)
        except RateLimitExceeded:
            logging.warning(f"Rate limit hit while updating DB at page {page}")
            raise e
        except Exception as e:
            logging.error(f"Error updating DB on page {page}: {e}")
            raise e
        if not activities:
            break
        run_activities = [
            a for a in activities 
            if a.get("type") == "Run"
            and not a.get("trainer",False)
        ] 
        if not run_activities:
            break
        if latest_activity and all(a['start_date'] <= latest_activity for a in run_activities):
            break
        ids = [a["id"] for a in run_activities]
        new_ids = ids_in_db(ids, database_file)
        new_acts = [a for a in run_activities if a["id"] in new_ids]

        if new_acts:
            database.insert_activities(new_acts, database_file)
            total_inserted += len(new_acts)
        page += 1
    logging.info(f"Finished update_database. Inserted {total_inserted} new activities.")
    database.mark_job_ran("update_database",LOG_FILE)
    
def remaining_requestst():
    used_15_min,used_daily = strava_api.get_request_usage()
    limit_daily = strava_api.LIMIT_DAILY
    limit_15_min = strava_api.LIMIT_15_MIN
    return limit_15_min-used_15_min, limit_daily - used_daily

def fetch_activity_streams(cfg:Config) -> None:
    logging.info("fetch_activity_streams: Running.")
    database_file = DB_FILE
    if not database_file.exists():
        logging.error("fetch_activity_streams: No database found.")
        raise RuntimeError(f"Initilise step failed to create datbase! Target file: {database_file}")
    stream_types = cfg.stream_types
    activity_ids = [
        a["id"] for a in database.query_db(
            "SELECT id FROM activities WHERE has_stream = 0",
            db_path=database_file
        )
    ]
    total_needed = len(activity_ids)
    logging.info(f"fetch_activity_streams: Need to fetch streams for {total_needed} activities")
    remaining_15_min, remaining_daily = remaining_requestst()
    if remaining_15_min <= 0:
        logging.warning(f"fetch_activity_streams: No remaining requests per 15 min: {remaining_15_min}")
        return
    if remaining_daily <= 0:
        logging.warning(f"fetch_activity_streams: No daily requests remaining: {remaining_daily}")
        return
    
    chunk_size = min(remaining_daily,remaining_15_min,len(activity_ids))
    to_process = activity_ids[:chunk_size]

    logging.info(f"fetch_activity_streams: Processing {len(to_process)} activities this run.")

    for activity_id in to_process:
        try:
            logging.info(f"fetch_activity_streams: Getting activity stream for id: {activity_id}")
            streams,resp = strava_api.get_streams(activity_id,stream_types)
        except RateLimitExceeded as e:
            logging.error(f"fetch_activity_streams: Exceeded read rate, exiting early! {e}")
            return
        except Exception as e:
            logging.error(f"fetch_activity_streams: Encountered exception {e}")
            return

        database.insert_stream(activity_id,streams,database_file)
        

    


if __name__ == "__main__":
    print("Running Main")
    cfg = load_config()
    DB_FILE = Path(cfg.data_dir)/"raw"/cfg.database_name
    LOG_FILE = Path(cfg.log_dir)/"pipeline.db"
    initilise_database(cfg)
    strava_api.set_log_dir(cfg.log_dir)
    fetch_all_activities(cfg)
    update_database(cfg)
    fetch_activity_streams(cfg)