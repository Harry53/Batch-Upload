import datetime
import subprocess

from .extensions import db
from .models import BatchJob
from .services import create_s3_links_from_batch_files, log_activity, mount_nas_path, unmount_nas_path, verify_nas_path, verify_s3_access

_app = None


def init_task_app(app):
    global _app
    _app = app


def _app_context():
    if _app is None:
        raise RuntimeError("Task app has not been initialized")
    return _app.app_context()


def run_script_task(job_id, cmd, log_file):
    with _app_context():
        job = db.session.get(BatchJob, job_id)
        job.status = 'Running'
        job.start_time = datetime.datetime.now()
        db.session.commit()
        try:
            with open(log_file, "a") as f:
                f.write(f"\n--- EXECUTION START: {datetime.datetime.now()} ---\n")
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                job.pid = proc.pid
                db.session.commit()
                for line in proc.stdout:
                    f.write(line)
                proc.wait()
            db.session.refresh(job)
            if job.status != 'Cancelled':
                job.status = 'Completed' if proc.returncode == 0 else 'Failed'
                job.completion_time = datetime.datetime.now()
                if job.status == 'Completed' and job.batch_number and job.batch_number != 'CUSTOM':
                    job.s3_link = create_s3_links_from_batch_files(job.batch_number, job.batch_type)
            log_activity(f"Execution finished: ticket={job.ticket_id}, status={job.status}, return_code={proc.returncode}")
        except Exception as e:
            job.status = 'Failed'
            job.completion_time = datetime.datetime.now()
            with open(log_file, "a") as f:
                f.write(f"CRITICAL ERROR: {str(e)}")
            log_activity(f"Execution failed: ticket={job.ticket_id}, error={str(e)}")
        db.session.commit()


def run_universal_transfer_task(job_id, mode, nas_path, s3_path, aws_profile, log_file):
    mount_dir = None
    with _app_context():
        job = db.session.get(BatchJob, job_id)
        job.status = 'Running'
        job.start_time = datetime.datetime.now()
        db.session.commit()
        try:
            with open(log_file, "a") as f:
                f.write(f"\n--- UNIVERSAL {mode.upper()} START: {datetime.datetime.now()} ---\n")
                nas_ok, nas_msg = verify_nas_path(nas_path)
                s3_ok, s3_msg = verify_s3_access(s3_path, aws_profile)
                f.write(f"[VERIFY NAS]\n{nas_msg}\n")
                f.write(f"[VERIFY S3]\n{s3_msg}\n")
                if not nas_ok or not s3_ok:
                    raise RuntimeError("Verification failed. Cannot proceed with transfer.")

                mount_dir, mounted_target = mount_nas_path(nas_path, writable=(mode == "download"))
                if mode == "upload":
                    cmd = ["/usr/local/bin/aws", "s3", "sync", mounted_target, s3_path, "--profile", aws_profile]
                else:
                    cmd = ["/usr/local/bin/aws", "s3", "sync", s3_path, mounted_target, "--profile", aws_profile]

                f.write(f"[COMMAND] {' '.join(cmd)}\n")
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                job.pid = proc.pid
                db.session.commit()
                for line in proc.stdout:
                    f.write(line)
                proc.wait()
                db.session.refresh(job)
                if job.status != 'Cancelled':
                    job.status = 'Completed' if proc.returncode == 0 else 'Failed'
                    job.completion_time = datetime.datetime.now()
            log_activity(f"Universal transfer finished: ticket={job.ticket_id}, mode={mode}, status={job.status}")
        except Exception as e:
            job.status = 'Failed'
            job.completion_time = datetime.datetime.now()
            with open(log_file, "a") as f:
                f.write(f"\nCRITICAL ERROR: {str(e)}\n")
            log_activity(f"Universal transfer failed: ticket={job.ticket_id}, mode={mode}, error={e}")
        finally:
            unmount_nas_path(mount_dir)
            db.session.commit()
