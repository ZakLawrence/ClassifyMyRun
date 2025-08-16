import luigi 
from api_tools.strava_api import rate_limit_guard, download_activity_streams
from database import get_con
from tasks.get_activities import FetchAllActivities
from luigi.configuration import get_config

cfg = get_config()
Data_Dir = cfg.get("project","data_dir")
Log_Dir = cfg.get("project","log_dir")

class GetStreamsForAllRuns(luigi.Task):
    dry_run = luigi.BoolParameter(default=False, description="Skip actual execution")

    def complete(self):
        if self.dry_run:
            return True
        return super().complete()
    
    def requires(self):
        return FetchAllActivities(dry_run=self.dry_run)
    
    def output(self):
        return luigi.LocalTarget(f"{Log_Dir}/streams_done.txt")

    def run(self):
        if self.dry_run:
            print(f"[Dry-run] Would fetch streams for activities in {Data_Dir}/strava.db")
            return
        
        db_path = f"{Data_Dir}/strava.db"
        stream_types = cfg.get("project","stream_types").split(",")

        with get_con(db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM activities WHERE has_stream = 0")
            rows = c.fetchall()
            activities_id = [row[0] for row in rows]

            total_needed = len(activities_id)
            print(f"Found {total_needed} activities needing streams!")
     
