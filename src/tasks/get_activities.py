import luigi
import utils
from luigi.configuration import get_config
from tasks.create_db import CreateDB
from api_tools.strava_api import get_activities, rate_limit_guard
from database import insert_activities,query_db
from datetime import datetime


cfg = get_config()
Data_Dir = cfg.get("project","data_dir")
Log_Dir = cfg.get("project","log_dir")

class FetchAllActivities(luigi.Task):
    per_page = luigi.IntParameter(default=cfg.get("project","per_page"))
    before = luigi.OptionalParameter(default=None)
    after = luigi.OptionalParameter(default=None)
    dry_run = luigi.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        rows = query_db(
            "SELECT * FROM activities ORDER BY start_date DESC LIMIT 1",
            db_path=f"{Data_Dir}/strava.db"
        )
        self.most_recent_activity = rows[0] if rows else None
        if self.most_recent_activity:
            self.most_recent_date = datetime.fromisoformat(self.most_recent_activity["start_date"])
        else: 
            self.most_recent_date = None

    def complete(self):
        if self.dry_run:
            return True
        return super().complete()

    def requires(self):
        return CreateDB(dry_run=self.dry_run)

    def output(self):
        if self.most_recent_activity:
            return luigi.LocalTarget(f"{Log_Dir}/fetched_activities_{self.most_recent_date.strftime("%d-%m-%Y")}.txt")
        else:
            return luigi.LocalTarget(f"{Log_Dir}/fetched_activities_{datetime.today().strftime("%d-%m-%Y")}.txt")

    def run(self):
        if self.dry_run:
            print("[Dry-run] Would fetch activities from Strava API")
            return
        
        page = 1
        total_inserted = 0
        while True:
            args = {k:v for (k,v) in {"page":page,"per_page":self.per_page,"before":self.before,"after":self.after}.items() if v is not None}
            activities, resp = get_activities(**args)
            if not rate_limit_guard(resp):
                break
            if not activities:
                break
                
            activity_ids = [a["id"] for a in query_db(
                "SELECT * FROM activities"
            )]

            run_activities = [
                a for a in activities 
                if a.get("type")=="Run" 
                and not a.get("trainer",False)
                and a.get("id") not in activity_ids 
            ]
                #and (
                #    self.most_recent_date is None or 
                #    datetime.fromisoformat(a["start_date"]) > self.most_recent_date
                #)]
            if not run_activities:
                continue 
            insert_activities(run_activities,f"{Data_Dir}/strava.db")

            total_inserted += len(run_activities)
            page += 1
        
        with self.output().open("w") as f:
            f.write(f"Inserted {total_inserted} activities into DB")