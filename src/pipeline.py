import luigi 
import json
import os 
import time 
from pathlib import Path
from datetime import datetime 
from database import init_db,get_con,insert_activities
from strava_api import * 

Data_Dir = Path("data/activities")
Model_Dir = Path("models")
Data_Dir.mkdir(exist_ok=True)
Model_Dir.mkdir(exist_ok=True)

class InitDB(luigi.Task):
    def output(self):
        return luigi.local_target(f"{Data_Dir}/db_initilised.txt")
    
    def run(self):
        init_db(f"{Data_Dir}/strava.db")
        with self.output().open("w") as f:
            f.write("DB Initilised")

class FetchAllActivities(luigi.Task):
    per_page = luigi.IntParameter(default=200)
    before = luigi.OptionalParameter(default=None)
    after = luigi.OptionalParameter(default=None)

    def requires(self):
        return InitDB()

    def output(self):
        return luigi.local_target(f"{Data_Dir}/fetched_all_activities.txt")
    
    def run(self):
        page = 1
        total_inserted = 0
        while True:
            activities, resp = get_activities(
                page=page,
                per_page=self.per_page,
                before=self.before,
                after=self.after
            )
            if rate_limit_guard(resp):
                break
            if not activities:
                break

            run_activities = [a for a in activities if a.get("type")=="Run"]
            insert_activities(run_activities,f"{Data_Dir}/strava.db")

            total_inserted += len(run_activities)
            page += 1
        
        with self.output().open("w") as f:
            f.write(f"Inserted {total_inserted} activities into DB")
