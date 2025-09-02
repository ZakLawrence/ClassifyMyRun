from api_tools.auth import get_session
import requests
import database
import json
import time
import datetime
from pathlib import Path
from utils import * 

BASE_URL = "https://www.strava.com/api/v3"
LOG_DIR: Path | None = None
LIMIT_15_MIN: int | None = None
LIMIT_DAILY: int | None = None

def set_log_dir(log_dir:Path) -> None:
    global LOG_DIR
    LOG_DIR = Path(log_dir)
    LOG_DIR.mkdir(parents=True,exist_ok=True)

def set_limits(response:requests.models.Response):
    global LIMIT_15_MIN, LIMIT_DAILY
    if not LIMIT_15_MIN or not LIMIT_DAILY:
        LIMIT_15_MIN, LIMIT_DAILY = map(int, response.headers["x-ratelimit-limit"].split(','))

def cache_response(response:requests.models.Response) -> None :
    log_db_file = LOG_DIR / "requests.db"
    if not log_db_file.exists():
        database.init_db(
            db_path=log_db_file,
            table=
            """
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                headers TEXT NOT NULL
            )
            """
        )
    database.log_requests(response,log_db_file)

def get_request_usage():
    log_db_file = LOG_DIR / "requests.db"
    daily_requests = database.get_requests_today(log_db_file)
    used_daily = len(daily_requests)
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(minutes=15)
    used_15_min = sum(1 for r in daily_requests if datetime.datetime.fromisoformat(r["timestamp"]) >= cutoff )
    return (used_15_min,used_daily)

def rate_limit_guard(response:requests.models.Response):
    cache_response(response)
    #Read Rate Limits: 100 requests every 15 minutes, 1,000 daily
    local_limit_15_min = 100
    local_limit_daily = 1000

    if "x-ratelimit-limit" in response.headers:
        set_limits(response)
        used_15_min, used_daily = map(int,response.headers["x-ratelimit-usage"].split(","))
        local_limit_15_min = LIMIT_15_MIN
        local_limit_daily = LIMIT_DAILY
    else:
        used_15_min, used_daily = get_request_usage()
        
    if used_15_min >= local_limit_15_min - 5: 
        print("Near 15 min limit, sleeping for 60s ...")
        time.spleep(60)
    if used_daily >= local_limit_daily - 5:
        raise RateLimitExceeded("daily",used_daily,local_limit_daily)

def get_activities(before=None,after=None,page:int=1,per_page:int=200):
    session = get_session()
    url = f"{BASE_URL}/athlete/activities"
    response = session.get(url, params={"per_page": per_page, "page": page})
    response.raise_for_status()
    rate_limit_guard(response)
    return response.json(),response

def get_streams(activity_id:int,types,resolution=None):
    extra_params : dict[str,any] = {}
    if resolution is not None:
        extra_params["resolution"] = resolution
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}/streams"
    response = session.get(url, params={"keys":",".join(types),"key_by_key":True,**extra_params})
    response.raise_for_status()
    rate_limit_guard(response)
    return response.json(),response

def download_activity(activity_id, out_path):
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}"
    response = session.get(url,params={"include_all_efforts":True})
    response.raise_for_status()
    with open(out_path,"w") as f:
        json.dump(response.json(),f)
    rate_limit_guard(response)
    return response

def download_activity_laps(activity_id, out_path):
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}/laps"
    response = session.get(url)
    response.raise_for_status()
    with open(out_path,"w") as f:
        json.dump(response.json(),f)
    rate_limit_guard(response)
    return response

def download_activity_streams(activity_id:int,types,out_path=None,resolution=None):
    extra_params : dict[str,any] = {}
    if resolution is not None:
        extra_params["resolution"] = resolution
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}/streams"
    response = session.get(url, params={"keys":",".join(types),"key_by_key":True,**extra_params})
    response.raise_for_status()
    if out_path:
        with open(out_path,"w") as f:
            json.dump(response.json(),f)
    rate_limit_guard(response)
    return response.json(), response

def update_activity(activity_id: int, **kwargs):
    """
    Update a Strava activity with allowed fields only.
    Allowed fields: commute, trainer, hide_from_home, description, name,
                    type (deprecated), sport_type, gear_id
    """
    allowed_fields = {
        "commute",
        "trainer",
        "hide_from_home",
        "description",
        "name",
        "type",
        "sport_type",
        "gear_id",
    }
    
    invalid_keys = set(kwargs) - allowed_fields
    if invalid_keys:
        raise ValueError(f"Invalid fields for update: {invalid_keys}")

    activity_kwargs = {k:v for k,v in kwargs.items() if k in allowed_fields}

    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}"
    response = session.put(url,params=activity_kwargs)
    response.raise_for_status()
    rate_limit_guard(response)
    return response
