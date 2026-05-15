import datetime
import os
import shutil
import socket
import subprocess
import tempfile
import time
from functools import wraps

from flask import flash, redirect, request, url_for
from flask_login import current_user, logout_user

from .config import WEB_LOG
from .extensions import db
from .models import BatchJob, UserActivity


def log_activity(action):
    try:
        if current_user.is_authenticated:
            act = UserActivity(username=current_user.username, action=action)
            db.session.add(act)
            db.session.commit()
            with open(WEB_LOG, "a") as f:
                f.write(f"[{datetime.datetime.now()}] {current_user.username}: {action}\n")
    except Exception:
        pass


def get_client_info():
    ip = request.remote_addr
    try:
        hostname = socket.gethostbyaddr(ip)[0]
    except Exception:
        hostname = "Unknown"
    return ip, hostname


def role_required(allowed_roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if current_user.is_disabled:
                logout_user()
                return redirect(url_for('login'))
            if current_user.role not in allowed_roles:
                flash('Insufficient Permissions', 'danger')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def get_next_ticket_id(prefix):
    last_job = BatchJob.query.filter(BatchJob.ticket_id.like(f"{prefix}-%")).order_by(BatchJob.id.desc()).first()
    if not last_job:
        return f"{prefix}-1000"
    try:
        last_num = int(last_job.ticket_id.split('-')[1])
        return f"{prefix}-{last_num + 1}"
    except Exception:
        return f"{prefix}-1000"


def create_s3_links_from_batch_files(batch_number, batch_type):
    links = []
    paths = {
        'studio': f"s3://testprivate01/Batch{batch_number}/",
        'bollywood': f"s3://testprivate01/Batch{batch_number}/",
        'llh': f"s3://amagicloud-samsungin/Media/S3/INSONO1/LL/Batch{batch_number}/"
    }
    if batch_type in paths:
        s3_base = paths[batch_type]
        try:
            cmd = f"/usr/local/bin/aws s3 ls {s3_base} --profile default 2>/dev/null || /usr/local/bin/aws s3 ls {s3_base} --profile amagicloud-samsung 2>/dev/null"
            result = subprocess.getoutput(cmd)
            if result:
                for line in result.split('\n'):
                    if line.strip():
                        filename = line.split()[-1].strip()
                        if 'amagicloud' in s3_base:
                            links.append(f"https://amagicloud-samsungin.s3.amazonaws.com/Media/S3/INSONO1/LL/Batch{batch_number}/{filename}")
                        else:
                            links.append(f"https://testprivate01.s3.amazonaws.com/Batch{batch_number}/{filename}")
        except Exception:
            pass
    return "; ".join(links) if links else ""


def parse_unc_path(path):
    clean = path.strip().replace("/", "\\")
    clean = clean.lstrip("\\")
    parts = [p for p in clean.split("\\") if p]
    if len(parts) < 2:
        return None, None
    server, share = parts[0], parts[1]
    subdir = "/".join(parts[2:]) if len(parts) > 2 else ""
    return f"//{server}/{share}", subdir


def mount_nas_path(path, writable=False):
    if not path:
        raise RuntimeError("NAS path is required")
    if not path.startswith("\\\\"):
        return None, path
    mount_dir = tempfile.mkdtemp(prefix="nas_mount_")
    remote, subdir = parse_unc_path(path)
    if not remote:
        shutil.rmtree(mount_dir, ignore_errors=True)
        raise RuntimeError("Invalid UNC path format. Expected \\\\server\\share\\folder")
    mount_opts = "rw,guest" if writable else "ro,guest"
    mount_cmd = ["/bin/mount", "-t", "cifs", remote, mount_dir, "-o", mount_opts]
    mount_proc = subprocess.run(mount_cmd, capture_output=True, text=True)
    if mount_proc.returncode != 0:
        shutil.rmtree(mount_dir, ignore_errors=True)
        raise RuntimeError(f"NAS mount failed: {mount_proc.stderr or mount_proc.stdout}")
    target = os.path.join(mount_dir, subdir) if subdir else mount_dir
    return mount_dir, target


def unmount_nas_path(mount_dir):
    if mount_dir and os.path.exists(mount_dir):
        subprocess.run(["/bin/umount", mount_dir], capture_output=True, text=True)
        shutil.rmtree(mount_dir, ignore_errors=True)


def verify_nas_path(path):
    mount_dir = None
    try:
        mount_dir, target = mount_nas_path(path, writable=False)
        out = subprocess.run(["/bin/ls", "-la", target], capture_output=True, text=True)
        success = out.returncode == 0
        msg = out.stdout if success else (out.stderr or out.stdout)
        return success, f"NAS check {'passed' if success else 'failed'}\n{msg}"
    except Exception as e:
        return False, f"NAS verify error: {e}"
    finally:
        unmount_nas_path(mount_dir)


def verify_s3_access(s3_path, aws_profile):
    if not s3_path.lower().startswith("s3://"):
        return False, "S3 path must start with s3://"
    list_cmd = ["/usr/local/bin/aws", "s3", "ls", s3_path, "--profile", aws_profile]
    list_proc = subprocess.run(list_cmd, capture_output=True, text=True)
    temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        temp_file.write(f"verify {datetime.datetime.now()}\n")
        temp_file.close()
        verify_key = f"{s3_path.rstrip('/')}/.verify_{int(time.time())}.txt"
        cp_cmd = ["/usr/local/bin/aws", "s3", "cp", temp_file.name, verify_key, "--profile", aws_profile]
        rm_cmd = ["/usr/local/bin/aws", "s3", "rm", verify_key, "--profile", aws_profile]
        cp_proc = subprocess.run(cp_cmd, capture_output=True, text=True)
        rm_proc = subprocess.run(rm_cmd, capture_output=True, text=True)
    finally:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
    success = list_proc.returncode == 0 and cp_proc.returncode == 0 and rm_proc.returncode == 0
    logs = [
        f"LIST: {(list_proc.stdout or list_proc.stderr).strip()}",
        f"UPLOAD TEST: {(cp_proc.stdout or cp_proc.stderr).strip()}",
        f"CLEANUP TEST: {(rm_proc.stdout or rm_proc.stderr).strip()}",
    ]
    return success, "\n".join(logs)
