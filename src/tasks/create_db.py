import luigi
from database import init_db
from luigi.configuration import get_config

cfg = get_config()
Data_Dir = cfg.get("project","data_dir")
Log_Dir = cfg.get("project","log_dir")

class CreateDB(luigi.Task):
    dry_run = luigi.BoolParameter(default=False, description="Skip actual execution")

    def complete(self):
        if self.dry_run:
            return True
        return super().complete()

    def output(self):
        return luigi.LocalTarget(f"{Log_Dir}/strava_db.txt")
    
    def run(self):
        if self.dry_run:
            print("[Dry-run] Would initilise the database")
            return
        
        init_db(f"{Data_Dir}/strava.db")
        with self.output().open("w") as f:
            f.write("DB Initilised")