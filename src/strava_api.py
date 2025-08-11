from auth import get_session
import requests
import json
import time

BASE_URL = "https://www.strava.com/api/v3"

def rate_limit_guard(response):
    used_15_min, used_daily = response.headers["x-ratelimit-usage"].split(",")
    limit_15_min, limit_daily = response.headers["x-ratelimit-limit"].split(",")
    if used_15_min >= limit_15_min - 5: 
        print("Near 15 min limit, sleeping for 60s ...")
        time.spleep(60)
    if used_daily >= limit_daily - 5:
        print("Near daily limit, stopping code execution")
        return False 
    return True

def get_activities(before=None,after=None,page=1,per_page=200):
    session = get_session()
    url = f"{BASE_URL}/athlete/activities"
    response = session.get(url, params={"per_page": per_page, "page": page})
    response.raise_for_status()
    return response.json(),response

def download_full_activity(activity_id, out_path):
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}"
    response = session.get(url,params={"include_all_efforts":True})
    response.raise_for_status()
    with open(out_path,"w") as f:
        json.dump(response.json(),f)
    return response

def download_activity_laps(activity_id, out_path):
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}/laps"
    response = session.get(url)
    response.raise_for_status()
    with open(out_path,"w") as f:
        json.dump(response.json(),f)
    return response

def download_activity_streams(activity_id:int,types,out_path,resolution=None):
    extra_params : dict[str,any] = {}
    if resolution is not None:
        extra_params["resolution"] = resolution
    session = get_session()
    url = f"{BASE_URL}/activities/{activity_id}/streams"
    response = session.get(url, params={"keys":",".join(types),"key_by_key":True,**extra_params})
    response.raise_for_status()
    with open(out_path,"w") as f:
        json.dump(response.json(),f)
    return response

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
    return response


if __name__ == "__main__":
    request_page = 1
    #while True:
    activities_data, _ = get_activities(page=request_page)
    print(len(activities_data))

    update_activity(activities_data[2]["id"],description="Test update description")
    
    #download_full_activity(activities_data[0]["id"], f"data/{activities_data[0]["id"]}_full.json")
    #download_activity_laps(activities_data[0]["id"], f"data/{activities_data[0]["id"]}_laps.json")
    #download_activity_streams(activities_data[0]["id"],["distance","altitude","velocity_smooth","heartrate","cadence","grade_smooth"],f"data/{activities_data[0]["id"]}_stream.json",resolution="low")
    
    #activity_id = activities_data[0]["id"]
    #gpx_file = download_gpx(activity_id,"")
    #print(gpx_file)
    
    #for activity in activities_data:
    #    print(activity["name"])
    #    
    #    #if len(activities_data)
    #    #request_page += 1        
