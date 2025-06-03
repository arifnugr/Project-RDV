import time
import schedule
import subprocess
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scheduler.log")
    ]
)
logger = logging.getLogger("scheduler")

class JobScheduler:
    def __init__(self):
        self.jobs = {}
        
    def add_job(self, job_name, interval_minutes, command, at_time=None):
        """Add a job to the scheduler
        
        Args:
            job_name (str): Name of the job
            interval_minutes (int): Interval in minutes
            command (str): Command to execute
            at_time (str, optional): Specific time to run (e.g., "10:30")
        """
        self.jobs[job_name] = {
            'interval': interval_minutes,
            'command': command,
            'at_time': at_time,
            'last_run': None
        }
        
        # Schedule the job
        if at_time:
            schedule.every().day.at(at_time).do(self._run_job, job_name=job_name)
            logger.info(f"Job '{job_name}' scheduled to run daily at {at_time}")
        else:
            schedule.every(interval_minutes).minutes.do(self._run_job, job_name=job_name)
            logger.info(f"Job '{job_name}' scheduled to run every {interval_minutes} minutes")
        
    def _run_job(self, job_name):
        """Run a job and update its last run time"""
        job = self.jobs.get(job_name)
        if not job:
            logger.warning(f"Job '{job_name}' not found")
            return
        
        logger.info(f"Running job '{job_name}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            result = subprocess.run(
                job['command'], 
                shell=True, 
                capture_output=True, 
                text=True,
                env=dict(os.environ)  # Pass current environment variables
            )
            if result.returncode == 0:
                logger.info(f"Job '{job_name}' completed successfully")
                if result.stdout:
                    logger.info(f"Output: {result.stdout[:200]}...")
            else:
                logger.error(f"Job '{job_name}' failed with error: {result.stderr}")
                
            # Update last run time
            self.jobs[job_name]['last_run'] = datetime.now()
        except Exception as e:
            logger.error(f"Error running job '{job_name}': {e}")
    
    def run_pending(self):
        """Run all pending jobs"""
        schedule.run_pending()
    
    def run_continuously(self, interval=1):
        """Run the scheduler continuously
        
        Args:
            interval (int): Sleep interval in seconds
        """
        logger.info(f"Starting scheduler with {len(self.jobs)} jobs")
        try:
            while True:
                self.run_pending()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
    
    def list_jobs(self):
        """List all scheduled jobs"""
        logger.info("\nScheduled Jobs:")
        logger.info("-" * 80)
        logger.info(f"{'Job Name':<20} {'Interval (min)':<15} {'At Time':<10} {'Last Run':<25} {'Command'}")
        logger.info("-" * 80)
        for name, job in self.jobs.items():
            last_run = job['last_run'].strftime('%Y-%m-%d %H:%M:%S') if job['last_run'] else "Never"
            at_time = job.get('at_time', 'N/A')
            logger.info(f"{name:<20} {job['interval']:<15} {at_time:<10} {last_run:<25} {job['command']}")


if __name__ == "__main__":
    # Example usage
    scheduler = JobScheduler()
    
    # Add jobs
    scheduler.add_job("export_csv", 5, "python -c \"from etl.exporter import export_to_csv; export_to_csv()\"")
    scheduler.add_job("spark_batch", 15, "python etl/spark_integration.py")
    
    # Add daily job at specific time
    scheduler.add_job("daily_report", 0, "python -c \"from etl.exporter import export_daily_report\"", at_time="00:00")
    
    # List all jobs
    scheduler.list_jobs()
    
    # Run the scheduler
    scheduler.run_continuously()