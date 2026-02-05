import os
import datetime
import subprocess
import threading
import json
import socket
import signal
import shutil
from functools import wraps
from flask import Flask, render_template_string, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_bcrypt import Bcrypt
from flask_apscheduler import APScheduler
from sqlalchemy import text, inspect
from sqlalchemy.exc import IntegrityError
import time

# ==========================================
# CONFIGURATION
# ==========================================
BASE_DIR = "/var/www/html/batch-web"
LOG_DIR = f"{BASE_DIR}/logs"
JOB_LOG_DIR = f"{LOG_DIR}/jobs"
WEB_LOG = f"{LOG_DIR}/web.log"
ARCHIVE_DIR = f"{LOG_DIR}/archive"
DB_FILE = f"sqlite:///{BASE_DIR}/batch_system.db"
BACKUP_DIR = f"{BASE_DIR}/backups"

os.makedirs(JOB_LOG_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
if not os.path.exists(WEB_LOG): open(WEB_LOG, 'a').close()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'GlobalBatchKey2026'
app.config['SQLALCHEMY_DATABASE_URI'] = DB_FILE
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
scheduler = APScheduler()
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# ==========================================
# DATABASE MODELS
# ==========================================
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)
    role = db.Column(db.String(20), default='Viewer')
    is_online = db.Column(db.Boolean, default=False)
    is_disabled = db.Column(db.Boolean, default=False)
    last_login = db.Column(db.DateTime, nullable=True)
    last_activity = db.Column(db.DateTime, default=datetime.datetime.now)
    last_ip = db.Column(db.String(50), nullable=True)        # New
    last_hostname = db.Column(db.String(100), nullable=True) # New

class BatchJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.String(50))
    batch_type = db.Column(db.String(50))
    batch_number = db.Column(db.String(50))
    status = db.Column(db.String(20)) # Running, Completed, Failed, Scheduled, Cancelled
    log_file_path = db.Column(db.String(200))
    triggered_by = db.Column(db.String(50))
    start_time = db.Column(db.DateTime, default=datetime.datetime.now)
    scheduled_time = db.Column(db.DateTime, nullable=True)
    completion_time = db.Column(db.DateTime, nullable=True)
    s3_link = db.Column(db.String(500), nullable=True)
    estimated_time = db.Column(db.String(50), nullable=True)
    pid = db.Column(db.Integer, nullable=True)               # New: To kill process
    source_path = db.Column(db.String(300), nullable=True)   # New: For LLH/S3
    dest_path = db.Column(db.String(300), nullable=True)     # New: For LLH/S3

class UserActivity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50))
    action = db.Column(db.String(255))
    timestamp = db.Column(db.DateTime, default=datetime.datetime.now)

class ArchiveData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    archive_ref_id = db.Column(db.String(100))
    archived_at = db.Column(db.DateTime, default=datetime.datetime.now)
    data_range = db.Column(db.String(100))
    # Storing data as JSON strings to keep main DB light
    job_data = db.Column(db.Text)
    activity_data = db.Column(db.Text)

@login_manager.user_loader
def load_user(user_id):
    user = db.session.get(User, int(user_id))
    if user and user.is_disabled: return None
    return user

# ==========================================
# HELPERS
# ==========================================
def log_activity(action):
    try:
        if current_user.is_authenticated:
            act = UserActivity(username=current_user.username, action=action)
            db.session.add(act)
            db.session.commit()
            with open(WEB_LOG, "a") as f:
                f.write(f"[{datetime.datetime.now()}] {current_user.username}: {action}\n")
    except: pass

def get_client_info():
    ip = request.remote_addr
    try:
        hostname = socket.gethostbyaddr(ip)[0]
    except:
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
    if not last_job: return f"{prefix}-1000"
    try:
        last_num = int(last_job.ticket_id.split('-')[1])
        return f"{prefix}-{last_num + 1}"
    except: return f"{prefix}-1000"

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
        except: pass
    return "; ".join(links) if links else ""

def run_script_task(job_id, cmd, log_file):
    with app.app_context():
        job = db.session.get(BatchJob, job_id)
        job.status = 'Running'
        job.start_time = datetime.datetime.now()
        db.session.commit()
        try:
            with open(log_file, "a") as f:
                f.write(f"\n--- EXECUTION START: {datetime.datetime.now()} ---\n")
                # Store PID to allow cancellation
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                job.pid = proc.pid
                db.session.commit()

                for line in proc.stdout: f.write(line)
                proc.wait()

            # Refresh job from DB in case it was cancelled externally
            db.session.refresh(job)
            if job.status != 'Cancelled':
                job.status = 'Completed' if proc.returncode == 0 else 'Failed'
                job.completion_time = datetime.datetime.now()
                if job.status == 'Completed' and job.batch_number and job.batch_number != 'CUSTOM':
                    job.s3_link = create_s3_links_from_batch_files(job.batch_number, job.batch_type)
        except Exception as e:
            job.status = 'Failed'
            job.completion_time = datetime.datetime.now()
            with open(log_file, "a") as f: f.write(f"CRITICAL ERROR: {str(e)}")
        db.session.commit()

# ==========================================
# UI TEMPLATES
# ==========================================
BASE_LAYOUT = """
<!doctype html><html><head><title>Batch System</title>
<meta http-equiv="refresh" content="60">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
    body { background:#f4f7f6; font-size: 0.85rem; }
    .sidebar { min-height:100vh; background:#1a252f; color:white; padding:20px 0; position: fixed; width: 240px; z-index: 1000; }
    .sidebar a { color:#bdc3c7; text-decoration:none; padding:12px 25px; display:block; }
    .sidebar a:hover, .sidebar a.active { background:#2c3e50; color:white; border-left:4px solid #3498db; }
    .user-profile { padding:15px 20px; border-bottom:1px solid #34495e; text-align:center; }
    .profile-img { width:50px; height:50px; border-radius:50%; background:#3498db; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; margin:0 auto 10px; }
    .main-content { margin-left: 240px; padding: 30px; margin-bottom: 60px; }
    .card { border:none; box-shadow: 0 0.125rem 0.25rem rgba(0,0,0,0.075); margin-bottom: 20px; border-radius: 10px; }
    .btn-xs { padding: 1px 5px; font-size: 0.75rem; }
    .table-responsive-scroll { max-height: 400px; overflow-y: auto; }
    .help-text { font-size: 0.7rem; color: #6c757d; display: block; margin-top: 2px; }
    footer { position: fixed; bottom: 0; width: calc(100% - 240px); left: 240px; background: #34495e; color: white; text-align: center; padding: 10px 0; font-size: 0.8rem; z-index: 999; }
</style>
<script>
// Global Click Logger
document.addEventListener('click', function(e) {
    // Attempt to identify the element clicked
    let label = e.target.innerText || e.target.id || e.target.className || e.target.tagName;
    if(label.length > 50) label = label.substring(0, 50) + "...";

    fetch("{{ url_for('log_click_event') }}", {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ element: label, url: window.location.pathname })
    });
});
</script>
</head>
<body><div class="sidebar">
    <div class="user-profile">
        <div class="profile-img">{{ current_user.username[0].upper() }}</div>
        <div><strong>{{ current_user.username }}</strong></div>
        <small class="opacity-75">{{ current_user.role }}</small>
    </div>
    <h5 class="text-center mb-4"><i class="fas fa-layer-group"></i> CSS BATCH OPS</h5>
    <a href="{{ url_for('dashboard') }}" class="{{ 'active' if request.endpoint == 'dashboard' }}"><i class="fas fa-home me-2"></i> Dashboard</a>
    <a href="{{ url_for('execution_page') }}" class="{{ 'active' if request.endpoint == 'execution_page' }}"><i class="fas fa-play me-2"></i> Execution</a>
    <a href="{{ url_for('user_mgmt') }}" class="{{ 'active' if request.endpoint == 'user_mgmt' }}"><i class="fas fa-users me-2"></i> Users</a>
    <a href="{{ url_for('admin_tools') }}" class="{{ 'active' if request.endpoint == 'admin_tools' }}"><i class="fas fa-tools me-2"></i> Admin Tools</a>
    <hr><a href="{{ url_for('logout_route') }}" class="text-danger"><i class="fas fa-sign-out-alt me-2"></i> Logout</a>
</div>
<div class="main-content">
    {% with m = get_flashed_messages(with_categories=true) %}{% if m %}{% for c,msg in m %}<div class="alert alert-{{c}} small">{{msg}}</div>{% endfor %}{% endif %}{% endwith %}
    {% block content %}{% endblock %}
</div>
<footer>&copy; Shemaroo Tech Team - 2026<sup>TM</sup></footer></body></html>
"""

LOGIN_PAGE = """
<!doctype html><html><head><title>Login</title><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style> body { background: #2c3e50; display: flex; align-items: center; justify-content: center; height: 100vh; }
.login-card { background: white; padding: 40px; border-radius: 15px; width: 350px; box-shadow: 0 10px 25px rgba(0,0,0,0.3); } </style></head>
<body><div class="login-card">
    <h4 class="text-center mb-4">Batch Management Portal</h4>
    <form method="post"><div class="mb-3"><label>Username</label><input name="username" class="form-control" required></div>
    <div class="mb-4"><label>Password</label><input type="password" name="password" class="form-control" required></div>
    <button class="btn btn-primary w-100">Login</button></form>
</div></body></html>
"""

DASHBOARD_PAGE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-9"><div class="card"><div class="card-body"><h6>Job Status Trends (Last 7 Days)</h6><canvas id="jobChart" height="90"></canvas></div></div></div>
    <div class="col-md-3">
        <div class="card bg-success text-white mb-2"><div class="card-body py-2"><span>Completed</span><h3>{{ comp_count }}</h3></div></div>
        <div class="card bg-danger text-white mb-2"><div class="card-body py-2"><span>Failed</span><h3>{{ fail_count }}</h3></div></div>
        <div class="card bg-warning text-white mb-2"><div class="card-body py-2"><span>Scheduled</span><h3>{{ sched_count }}</h3></div></div>
        <div class="card bg-primary text-white mb-2"><div class="card-body py-2"><span>Active Users</span><h3>{{ active_users }}</h3></div></div>
    </div>
</div>
<div class="card"><div class="card-header bg-white"><b>Recent Job History</b></div>
<div class="card-body p-0 table-responsive-scroll"><table class="table table-sm mb-0">
    <thead class="table-light"><tr><th>Ticket</th><th>Type</th><th>Batch #</th><th>Status</th><th>Start</th><th>Schedule</th><th>End</th><th>Action</th><th>Log</th></tr></thead>
    <tbody>{% for j in history %}<tr><td>{{ j.ticket_id }}</td><td>{{ j.batch_type }}</td><td>{{ j.batch_number }}</td>
    <td><span class="badge {{ 'bg-success' if j.status=='Completed' else 'bg-danger' if j.status=='Failed' else 'bg-warning' if j.status=='Scheduled' else 'bg-secondary' }}">{{ j.status }}</span></td>
    <td>{{ j.start_time.strftime('%d-%m-%Y %H:%M') if j.start_time else '-' }}</td>
    <td>{{ j.scheduled_time.strftime('%d-%m-%Y %H:%M') if j.scheduled_time else '-' }}</td>
	<td>{{ j.completion_time.strftime('%d-%m-%Y %H:%M') if j.completion_time else '-' }}</td>
    <td>
        {% if j.status in ['Running', 'Scheduled'] %}
        <a href="{{ url_for('cancel_job', j_id=j.id) }}" class="btn btn-xs btn-danger" onclick="return confirm('Are you sure you want to cancel this job?')">Cancel</a>
        {% else %}-{% endif %}
    </td>
    <td><a href="{{ url_for('view_job_log', j_id=j.id) }}" target="_blank" class="btn btn-xs btn-outline-primary">View</a></td>
    </tr>{% endfor %}</tbody>
</table></div></div>
<script>
new Chart(document.getElementById('jobChart'), {
    type: 'line',
    data: {
        labels: {{ labels|safe }},
        datasets: [
            { label: 'Completed', data: {{ data_comp|safe }}, borderColor: '#27ae60', backgroundColor: 'rgba(39, 174, 96, 0.1)', fill: true, tension: 0.3 },
            { label: 'Failed', data: {{ data_fail|safe }}, borderColor: '#e74c3c', backgroundColor: 'rgba(231, 76, 60, 0.1)', fill: true, tension: 0.3 }
        ]
    }, options: { scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
});
</script>
{% endblock %}
"""

EXECUTION_PAGE = """
{% extends "base" %}
{% block content %}
<div class="card mt-2"><div class="card-header bg-warning"><b>Job Reschedule</b></div><div class="card-body">
<form method="post" action="{{ url_for('reschedule_job_route') }}"><div class="row g-2">
    <div class="col-md-3">
        <input name="ticket_id" class="form-control form-control-sm" placeholder="Ticket ID (e.g. ST-1001)" required>
        <span class="help-text">Enter existing Scheduled/Failed ticket ID</span>
    </div>
    <div class="col-md-3"><select name="panel" class="form-select form-select-sm"><option value="studio">Studio-Stagging</option><option value="bollywood">SW-Bollywood</option><option value="llh">LLH-Upload</option><option value="s3">S3-Upload</option></select></div>
    <div class="col-md-4"><input type="datetime-local" name="new_sched" class="form-control form-control-sm" required></div>
    <div class="col-md-2"><button class="btn btn-dark btn-sm w-100">Update</button></div>
</div></form></div></div>

<div class="row">
    <div class="col-md-6">
        <div class="card"><div class="card-header"><b>Studio-Stagging Batch</b></div><div class="card-body">
        <form method="post"><input type="hidden" name="panel" value="studio"><div class="row g-2">
            <div class="col-6"><label>Ticket ID</label><input class="form-control" value="{{ ids.ST }}" readonly></div>
            <div class="col-6"><label>Batch #</label><input name="b_no" class="form-control" required><span class="help-text">e.g. 1045, 1046</span></div>
            <div class="col-6"><label>AWS Profile</label><input name="aws" class="form-control" value="default" readonly><span class="help-text">AWS CLI Profile name</span></div>
            <div class="col-6"><label>Schedule</label><input type="datetime-local" name="sched" class="form-control"><span class="help-text">Leave blank to run now</span></div>
            <div class="col-12"><label>Recipients</label><input name="rcp" class="form-control" value="cdnupdate@shemaroo.com,haraprasad.mishra@shemaroo.com" required><span class="help-text">Comma separated emails</span></div>
            <button class="btn btn-primary btn-sm mt-3 w-100">Run Studio Batch</button>
        </div></form></div></div>

        <div class="card"><div class="card-header"><b>LLH-Upload</b></div><div class="card-body">
        <form method="post"><input type="hidden" name="panel" value="llh"><div class="row g-2">
            <div class="col-6"><label>Ticket ID</label><input class="form-control" value="{{ ids.LH }}" readonly></div>
            <div class="col-6"><label>Batch #</label><input name="b_no" class="form-control" required><span class="help-text">Unique Batch Number</span></div>
            <div class="col-12"><label>Source Path</label>
                <input name="src_path" class="form-control" value="/mnt/CSS-LLH" placeholder="/mnt/CSS-LLH">
                <span class="help-text">NAS Share Path: \\\\192.168.0.150\\syndicationmaster</span>
            </div>
            <div class="col-12"><label>Destination Path</label>
                <input name="dst_path" class="form-control" value="s3://amagicloud-samsungin/Media/S3/INSONO1/LL/Movies_Club_LLH" placeholder="s3://amagicloud-samsungin/Media/S3/INSONO1/LL/Movies_Club_LLH">
                <span class="help-text">Default: s3://amagicloud-samsungin/Media/S3/INSONO1/LL/Movies_Club_LLH</span>
            </div>
            <div class="col-6"><label>AWS Profile</label><input name="aws" class="form-control" value="amagicloud-samsung" readonly></div>
            <div class="col-6"><label>Schedule</label><input type="datetime-local" name="sched" class="form-control"></div>
            <div class="col-12"><label>Recipients</label><input name="rcp" class="form-control" value="cdnupdate@shemaroo.com,haraprasad.mishra@shemaroo.com" required></div>
            <button class="btn btn-info btn-sm mt-3 w-100 text-white">Run LLH Upload</button>
        </div></form></div></div>
    </div>

    <div class="col-md-6">
        <div class="card"><div class="card-header"><b>SW-Bollywood Batch</b></div><div class="card-body">
        <form method="post"><input type="hidden" name="panel" value="bollywood"><div class="row g-2">
            <div class="col-6"><label>Ticket ID</label><input class="form-control" value="{{ ids.SW }}" readonly></div>
            <div class="col-6"><label>Batch #</label><input name="b_no" class="form-control" required><span class="help-text">e.g. 501, 502</span></div>
            <div class="col-6"><label>AWS Profile</label><input name="aws" class="form-control" value="default"></div>
            <div class="col-6"><label>Schedule</label><input type="datetime-local" name="sched" class="form-control"></div>
            <div class="col-12"><label>Recipients</label><input name="rcp" class="form-control" value="cdnupdate@shemaroo.com,haraprasad.mishra@shemaroo.com" required></div>
            <button class="btn btn-secondary btn-sm mt-3 w-100">Run Bollywood Batch</button>
        </div></form></div></div>
        <div class="card"><div class="card-header"><b>S3 Upload (Generic)</b></div><div class="card-body">
        <form method="post"><input type="hidden" name="panel" value="s3"><div class="row g-2">
            <div class="col-6"><label>Ticket ID</label><input class="form-control" value="{{ ids.S3 }}" readonly></div>
            <div class="col-6"><label>AWS Profile</label><input name="aws" class="form-control" value="default"></div>
            <div class="col-12"><label>Source Path</label><input name="src" class="form-control" required><span class="help-text">Local server path</span></div>
            <div class="col-12"><label>Destination Path</label><input name="dst" class="form-control" required><span class="help-text">S3 URI (s3://bucket/path)</span></div>
            <div class="col-6"><label>Schedule</label><input type="datetime-local" name="sched" class="form-control"></div>
            <div class="col-6"><label>Recipients</label><input name="rcp" class="form-control" value="cdnupdate@shemaroo.com,haraprasad.mishra@shemaroo.com" required></div>
            <button class="btn btn-dark btn-sm mt-3 w-100">Run Upload</button>
        </div></form></div></div>
    </div>
</div>
{% endblock %}
"""

ADMIN_TOOLS_PAGE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-12 mb-3"><div class="card border-primary"><div class="card-header bg-primary text-white"><b>Data Archival</b></div><div class="card-body">
    <form method="post" action="{{ url_for('archive_data_route') }}" class="row align-items-center">
        <div class="col-md-2"><label>Archive Period:</label></div>
        <div class="col-md-3">
            <select name="period" class="form-select" onchange="this.value=='custom'?document.getElementById('custom_range').style.display='flex':document.getElementById('custom_range').style.display='none'">
                <option value="1m">Older than 1 Month</option>
                <option value="3m">Older than 3 Months</option>
                <option value="6m">Older than 6 Months</option>
                <option value="1y">Older than 1 Year</option>
                <option value="custom">Custom Date Range</option>
            </select>
        </div>
        <div class="col-md-5" id="custom_range" style="display:none; gap:5px;">
            <input type="date" name="custom_start" class="form-control"> to <input type="date" name="custom_end" class="form-control">
        </div>
        <div class="col-md-2"><button class="btn btn-outline-primary w-100" onclick="return confirm('This will MOVE data and log files to archive. Continue?')">Start Archive</button></div>
    </form>
    </div></div></div>

    <div class="col-md-8"><div class="card"><div class="card-header bg-dark text-white"><b>Audit & Path Validation</b></div><div class="card-body">
    <form method="post" class="row g-2 mb-3">
        <input type="hidden" name="audit_action" value="1">
        <div class="col-auto"><input name="check_batch" class="form-control" placeholder="Batch Number" required></div>
        <div class="col-auto"><select name="audit_type" class="form-select">
            <option value="studio">Studio-Stagging</option><option value="bollywood">SW-Bollywood</option><option value="llh">LLH-Upload</option>
        </select></div><div class="col-auto"><button class="btn btn-dark">Validate Paths</button></div>
    </form>
    <form method="post" class="row g-2 mt-3">
        <input type="hidden" name="s3_link_action" value="1">
        <div class="col-auto"><input name="s3_batch" class="form-control" placeholder="Batch Number" required></div>
        <div class="col-auto"><select name="s3_type" class="form-select">
            <option value="studio">Studio-Stagging</option><option value="bollywood">SW-Bollywood</option><option value="llh">LLH-Upload</option>
        </select></div><div class="col-auto"><button class="btn btn-success">S3 Link Creation</button></div>
    </form>
    {% if s3_links %}<div class="mt-3"><h6>S3 Links Generated:</h6><pre class="bg-light p-2 small border" style="max-height:120px; overflow:auto;">{{ s3_links }}</pre></div>{% endif %}
    {% if audit_s3 or audit_nas %}<div class="row"><div class="col-md-6"><h6>S3 Listing</h6><pre class="bg-light p-2 small border" style="height:120px; overflow:auto;">{{ audit_s3 }}</pre></div>
    <div class="col-md-6"><h6>NAS Listing</h6><pre class="bg-light p-2 small border" style="height:120px; overflow:auto;">{{ audit_nas }}</pre></div></div>{% endif %}
    </div></div></div>

    <div class="col-md-4"><div class="card"><div class="card-header bg-secondary text-white"><b>Database Backup</b></div><div class="card-body">
    <form method="post" action="{{ url_for('backup_db') }}"><button class="btn btn-success w-100">Create Backup</button></form>
    {% if backup_file %}<div class="mt-2 p-2 bg-light small">Latest: {{ backup_file }}</div>{% endif %}
    <hr>
    <form method="post" enctype="multipart/form-data" action="{{ url_for('restore_db') }}">
        <input type="file" name="db_file" class="form-control form-control-sm" accept=".db">
        <button class="btn btn-warning btn-sm w-100 mt-2">Restore DB</button>
    </form></div></div></div>
</div>

<div class="card"><div class="card-header d-flex justify-content-between align-items-center">
    <b>Job History</b>
    <form class="d-flex gap-2">
        <input type="date" name="start_date" class="form-control form-control-sm" value="{{ request.args.get('start_date', '') }}">
        <input type="date" name="end_date" class="form-control form-control-sm" value="{{ request.args.get('end_date', '') }}">
        <button class="btn btn-sm btn-primary">Filter</button>
    </form>
</div>
<div class="card-body p-0 table-responsive-scroll">
<table class="table table-sm mb-0"><thead><tr><th>Ticket</th><th>Batch #</th><th>Type</th><th>Status</th><th>Start</th><th>Schedule</th><th>Completion</th><th>Action</th><th>Log</th></tr></thead>
<tbody>{% for h in history %}<tr>
    <td>{{ h.ticket_id }}</td><td>{{ h.batch_number }}</td><td>{{ h.batch_type }}</td>
    <td><span class="badge {{ 'bg-success' if h.status=='Completed' else 'bg-danger' if h.status=='Failed' else 'bg-warning' if h.status=='Scheduled' else 'bg-secondary' }}">{{ h.status }}</span></td>
    <td>{{ h.start_time.strftime('%Y-%m-%d %H:%M') if h.start_time else '-' }}</td>
    <td>{{ h.scheduled_time.strftime('%Y-%m-%d %H:%M') if h.scheduled_time else '-' }}</td>
    <td>{{ h.completion_time.strftime('%Y-%m-%d %H:%M') if h.completion_time else '-' }}</td>
    <td>{% if h.status in ['Running', 'Scheduled'] %}<a href="{{ url_for('cancel_job', j_id=h.id) }}" class="btn btn-xs btn-danger">Cancel</a>{% else %}-{% endif %}</td>
    <td><a href="{{ url_for('view_job_log', j_id=h.id) }}" target="_blank" class="btn btn-xs btn-outline-primary">Log</a></td>
</tr>{% endfor %}</tbody>
</table></div></div>

<div class="card"><div class="card-header d-flex justify-content-between align-items-center">
    <b>User Activity Log</b>
    <form class="d-flex gap-2" action="{{ url_for('admin_tools') }}">
        <input type="date" name="act_start" class="form-control form-control-sm" value="{{ request.args.get('act_start', '') }}">
        <input type="date" name="act_end" class="form-control form-control-sm" value="{{ request.args.get('act_end', '') }}">
        <button class="btn btn-sm btn-info text-white">Filter Log</button>
    </form>
</div>
<div class="card-body p-0 table-responsive-scroll"><table class="table table-sm mb-0"><thead class="table-light"><tr><th>User</th><th>Action</th><th>Time</th></tr></thead>
<tbody>{% for a in activities %}<tr><td>{{ a.username }}</td><td>{{ a.action }}</td>
<td>{{ a.timestamp.strftime('%Y-%m-%d %H:%M:%S') }}</td></tr>{% endfor %}</tbody></table></div></div>

<div class="card"><div class="card-header"><b>Web Log (Latest 200 lines)</b>
    <small class="text-muted d-block">Path: /var/www/html/batch-web/logs/web.log</small>
</div>
<div class="card-body">
    <pre class="bg-light p-2 small border" style="max-height:260px; overflow:auto; white-space:pre-wrap;">{{ web_log_tail }}</pre>
</div></div>
{% endblock %}
"""

USER_MGMT_PAGE = """
{% extends "base" %}
{% block content %}
<div class="card mb-4"><div class="card-header"><b>Create New User</b></div><div class="card-body">
<form method="post" action="{{ url_for('create_user') }}" class="row g-2">
    <div class="col-auto"><input name="username" class="form-control form-control-sm" placeholder="Username" required></div>
    <div class="col-auto"><input name="password" type="password" class="form-control form-control-sm" placeholder="Password" required></div>
    <div class="col-auto"><select name="role" class="form-select form-select-sm"><option>Viewer</option><option>Editor</option><option>Admin</option></select></div>
    <div class="col-auto"><button class="btn btn-sm btn-success">Create User</button></div>
</form></div></div>
<div class="card"><div class="card-header"><b>Active User Management</b></div><div class="card-body p-0 table-responsive-scroll">
<table class="table table-sm mb-0"><thead><tr><th>User</th><th>Role</th><th>Status</th><th>Last IP/Host</th><th>Disabled</th><th>Last Login</th><th>Actions</th></tr></thead>
<tbody>{% for u in users %}<tr>
    <td>{{ u.username }}</td><td>{{ u.role }}</td>
    <td><span class="badge {{ 'bg-success' if u.is_online else 'bg-secondary' }}">{{ 'Online' if u.is_online else 'Offline' }}</span></td>
    <td><small>{{ u.last_ip }}<br>{{ u.last_hostname }}</small></td>
    <td><span class="badge {{ 'bg-danger' if u.is_disabled else 'bg-success' }}">{{ 'Yes' if u.is_disabled else 'No' }}</span></td>
    <td>{{ u.last_login.strftime('%Y-%m-%d %H:%M:%S') if u.last_login else '-' }}</td>
	<td><form method="post" action="{{ url_for('update_user') }}" class="row g-1">
        <input type="hidden" name="u_id" value="{{ u.id }}">
        <div class="col-auto"><select name="new_role" class="form-select form-select-sm">
            <option value="Viewer" {{ 'selected' if u.role=='Viewer' }}>Viewer</option>
            <option value="Editor" {{ 'selected' if u.role=='Editor' }}>Editor</option>
            <option value="Admin" {{ 'selected' if u.role=='Admin' }}>Admin</option>
        </select></div>
        <div class="col-auto"><input name="new_pass" type="password" placeholder="New Pass" class="form-control form-control-sm"></div>
        <div class="col-auto"><button type="submit" name="disable" value="1" class="btn btn-xs btn-{{ 'success' if not u.is_disabled else 'danger' }}">{{ 'Enable' if u.is_disabled else 'Disable' }}</button></div>
        <div class="col-auto"><button class="btn btn-xs btn-primary">Update</button></div>
    </form></td></tr>{% endfor %}</tbody></table></div></div>
{% endblock %}
"""

# ==========================================
# ROUTES
# ==========================================
def render_page(t_str, **kwargs):
    content = t_str.replace('{% extends "base" %}', '').replace('{% block content %}', '').replace('{% endblock %}', '')
    return render_template_string(BASE_LAYOUT.replace('{% block content %}{% endblock %}', content), **kwargs)

@app.route('/log-click', methods=['POST'])
@login_required
def log_click_event():
    data = request.get_json(silent=True) or {}
    elem = data.get('element', 'Unknown')
    url = data.get('url', '')
    with open(WEB_LOG, "a") as f:
        f.write(f"[{datetime.datetime.now()}] {current_user.username} CLICKED: {elem} on {url}\n")
    return jsonify(status="success")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and not user.is_disabled and bcrypt.check_password_hash(user.password, request.form['password']):
            user.is_online = True
            user.last_login = datetime.datetime.now()
            user.last_activity = datetime.datetime.now()
            # Capture IP and Hostname
            ip, hostname = get_client_info()
            user.last_ip = ip
            user.last_hostname = hostname

            db.session.commit()
            login_user(user)
            log_activity(f"Logged In from {ip} ({hostname})")
            return redirect(url_for('dashboard'))
        flash('Invalid credentials or disabled account', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout_route():
    try:
        user = db.session.get(User, current_user.id)
        user.is_online = False
        db.session.commit()
    except: pass
    log_activity("Logged Out")
    logout_user()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    # Session timeout check
    try:
        user = db.session.get(User, current_user.id)
        if user.last_activity and (datetime.datetime.now() - user.last_activity).seconds > 3600:
            user.is_online = False
            db.session.commit()
            logout_user()
            return redirect(url_for('login'))
        user.last_activity = datetime.datetime.now()
        db.session.commit()
    except: pass

    history = BatchJob.query.order_by(BatchJob.id.desc()).limit(10).all()
    active_users = User.query.filter_by(is_online=True).count()

    today = datetime.datetime.now().date()
    labels = [(today - datetime.timedelta(days=i)).strftime('%m-%d') for i in range(6, -1, -1)]
    data_comp, data_fail = [], []
    for i in range(6, -1, -1):
        target_date = today - datetime.timedelta(days=i)
        data_comp.append(BatchJob.query.filter(BatchJob.status=='Completed', db.func.date(BatchJob.start_time)==target_date).count())
        data_fail.append(BatchJob.query.filter(BatchJob.status=='Failed', db.func.date(BatchJob.start_time)==target_date).count())

    return render_page(DASHBOARD_PAGE, history=history, active_users=active_users,
                       comp_count=BatchJob.query.filter_by(status='Completed').count(),
                       fail_count=BatchJob.query.filter_by(status='Failed').count(),
                       sched_count=BatchJob.query.filter_by(status='Scheduled').count(),
                       labels=labels, data_comp=data_comp, data_fail=data_fail)

@app.route('/admin-tools', methods=['GET', 'POST'])
@login_required
@role_required(['Admin'])
def admin_tools():
    s3_list, nas_list, s3_links_result = "", "", ""
    if request.method == 'POST':
        if 'audit_action' in request.form:
            b = request.form['check_batch']
            audit_type = request.form.get('audit_type', 'studio')
            paths = {
                'studio': {'s3': f"/usr/local/bin/aws s3 ls s3://testprivate01/Batch{b}/ --profile default 2>&1", 'nas': f"/bin/ls -lhR /mnt/studio-stagging/Batch{b}/ 2>&1"},
                'bollywood': {'s3': f"/usr/local/bin/aws s3 ls s3://testprivate01/Batch{b}/ --profile default 2>&1", 'nas': f"/bin/ls -lhR /mnt/SW_Bollywood/Batch{b}/ 2>&1"},
                'llh': {'s3': f"/usr/local/bin/aws s3 ls s3://amagicloud-samsungin/Media/S3/INSONO1/LL/Batch{b}/ --profile amagicloud-samsung 2>&1", 'nas': f"/bin/ls -lhR /mnt/SW_Bollywood/Bollywood_Plus_Movies/LLH/Batch{b}/ 2>&1"}
            }
            if audit_type in paths:
                s3_list = subprocess.getoutput(paths[audit_type]['s3'])
                nas_list = subprocess.getoutput(paths[audit_type]['nas'])
                log_activity(f"Audited Batch {b} for {audit_type}")

        elif 's3_link_action' in request.form:
            batch_num = request.form['s3_batch']
            s3_type = request.form.get('s3_type', 'studio')
            s3_links_result = create_s3_links_from_batch_files(batch_num, s3_type)
            log_activity(f"Created S3 links for Batch {batch_num} ({s3_type})")
            jobs = BatchJob.query.filter_by(batch_number=batch_num, batch_type=s3_type).all()
            for job in jobs: job.s3_link = s3_links_result
            db.session.commit()
            flash(f"S3 links updated for Batch {batch_num}", "success")

    # History Filter
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    history_query = BatchJob.query
    if start_date: history_query = history_query.filter(BatchJob.start_time >= datetime.datetime.strptime(start_date, '%Y-%m-%d'))
    if end_date: history_query = history_query.filter(BatchJob.start_time <= datetime.datetime.strptime(end_date, '%Y-%m-%d') + datetime.timedelta(days=1))
    history = history_query.order_by(BatchJob.id.desc()).limit(100).all()

    # Activity Filter
    act_start = request.args.get('act_start')
    act_end = request.args.get('act_end')
    activity_query = UserActivity.query
    if act_start: activity_query = activity_query.filter(UserActivity.timestamp >= datetime.datetime.strptime(act_start, '%Y-%m-%d'))
    if act_end: activity_query = activity_query.filter(UserActivity.timestamp <= datetime.datetime.strptime(act_end, '%Y-%m-%d') + datetime.timedelta(days=1))
    activities = activity_query.order_by(UserActivity.timestamp.desc()).limit(100).all()

    backups = [f for f in os.listdir(BACKUP_DIR) if f.endswith('.db')]
    backup_file = max(backups, key=lambda x: os.path.getctime(os.path.join(BACKUP_DIR, x))) if backups else None

    web_log_tail = ""
    try:
        with open(WEB_LOG, 'r') as f:
            web_log_tail = ''.join(f.readlines()[-200:])
    except Exception as e:
        web_log_tail = f"Unable to read web log: {e}"

    return render_page(ADMIN_TOOLS_PAGE, history=history, activities=activities,
                       backup_file=backup_file, audit_s3=s3_list, audit_nas=nas_list,
                       s3_links=s3_links_result, web_log_tail=web_log_tail)

@app.route('/archive-data', methods=['POST'])
@login_required
@role_required(['Admin'])
def archive_data_route():
    period = request.form['period']
    cutoff_date = None
    now = datetime.datetime.now()

    if period == '1m': cutoff_date = now - datetime.timedelta(days=30)
    elif period == '3m': cutoff_date = now - datetime.timedelta(days=90)
    elif period == '6m': cutoff_date = now - datetime.timedelta(days=180)
    elif period == '1y': cutoff_date = now - datetime.timedelta(days=365)
    elif period == 'custom':
        try:
            cutoff_date = datetime.datetime.strptime(request.form['custom_end'], '%Y-%m-%d')
        except:
            flash("Invalid Custom Date", "danger")
            return redirect(url_for('admin_tools'))

    if not cutoff_date:
        flash("Invalid Period", "danger")
        return redirect(url_for('admin_tools'))

    # Fetch Data to Archive
    jobs_to_archive = BatchJob.query.filter(BatchJob.start_time <= cutoff_date).all()
    acts_to_archive = UserActivity.query.filter(UserActivity.timestamp <= cutoff_date).all()

    if not jobs_to_archive and not acts_to_archive:
        flash("No data found to archive for this period.", "info")
        return redirect(url_for('admin_tools'))

    # Create Archive Folder
    unique_id = f"ARCHIVE_{now.strftime('%Y%m%d_%H%M%S')}"
    archive_path = f"{ARCHIVE_DIR}/{unique_id}"
    os.makedirs(archive_path, exist_ok=True)

    # Move Log Files
    for job in jobs_to_archive:
        if job.log_file_path and os.path.exists(job.log_file_path):
            try:
                shutil.move(job.log_file_path, f"{archive_path}/{os.path.basename(job.log_file_path)}")
            except Exception as e:
                print(f"Failed to move log: {e}")

    # Serialize Data to JSON for Archive Table
    job_json = json.dumps([{"id": j.id, "ticket": j.ticket_id, "start": str(j.start_time)} for j in jobs_to_archive])
    act_json = json.dumps([{"user": a.username, "action": a.action, "time": str(a.timestamp)} for a in acts_to_archive])

    # Save to Archive Table
    archive_entry = ArchiveData(archive_ref_id=unique_id, data_range=f"Before {cutoff_date}", job_data=job_json, activity_data=act_json)
    db.session.add(archive_entry)

    # Delete from Main Tables
    if jobs_to_archive:
        BatchJob.query.filter(BatchJob.start_time <= cutoff_date).delete()
    if acts_to_archive:
        UserActivity.query.filter(UserActivity.timestamp <= cutoff_date).delete()

    db.session.commit()
    flash(f"Archived {len(jobs_to_archive)} jobs and {len(acts_to_archive)} activities to {unique_id}", "success")
    log_activity(f"Ran Data Archival ({period})")
    return redirect(url_for('admin_tools'))

@app.route('/execution', methods=['GET', 'POST'])
@login_required
def execution_page():
    if request.method == 'POST':
        panel = request.form['panel']
        mapping = {
            'studio': ('ST', '/var/www/html/batch-web/scripts/Studio-Staging-Advanced.sh'),
            'bollywood': ('SW', '/var/www/html/batch-web/scripts/SW-Bollywood-Advanced.sh'),
            'llh': ('LH', '/var/www/html/batch-web/scripts/LLH-Upload-Advanced.sh'),
            's3': ('S3', '/var/www/html/batch-web/scripts/generic-s3.sh')
        }
        prefix, script = mapping[panel]
        t_id = get_next_ticket_id(prefix)

        # Determine Arguments based on panel
        if panel == 's3':
            b_no = "CUSTOM"
            cmd = [script, request.form['aws'], request.form['src'], request.form['dst'], request.form['rcp']]
            new_j = BatchJob(source_path=request.form['src'], dest_path=request.form['dst'])
        elif panel == 'llh':
            b_no = request.form['b_no']
            src = request.form.get('src_path', '/mnt/CSS-LLH/LLH/Revised')
            dst = request.form.get('dst_path', 's3://amagicloud-samsungin/Media/S3/INSONO1/LL/Movies_Club_LLH')
            cmd = [script, t_id, b_no, request.form['aws'], request.form['rcp'], src, dst]
            new_j = BatchJob(source_path=src, dest_path=dst)
        else:
            b_no = request.form['b_no']
            cmd = [script, t_id, b_no, request.form['aws'], request.form['rcp']]
            new_j = BatchJob()

        log_f = f"{JOB_LOG_DIR}/job_{t_id}.log"
        new_j.ticket_id = t_id
        new_j.batch_type = panel
        new_j.batch_number = b_no
        new_j.log_file_path = log_f
        new_j.triggered_by = current_user.username

        sched_val = request.form.get('sched')
        if sched_val:
            dt = datetime.datetime.fromisoformat(sched_val)
            new_j.status, new_j.scheduled_time = 'Scheduled', dt
            db.session.add(new_j); db.session.commit()
            scheduler.add_job(id=f"j_{new_j.id}", func=run_script_task, trigger='date', run_date=dt, args=[new_j.id, cmd, log_f])
            flash(f"Job {t_id} scheduled for {sched_val}", "info")
            log_activity(f"Scheduled job {t_id}")
        else:
            new_j.status = 'Running'
            db.session.add(new_j); db.session.commit()
            threading.Thread(target=run_script_task, args=(new_j.id, cmd, log_f)).start()
            flash(f"Job {t_id} started immediately", "success")
            log_activity(f"Started job {t_id}")
        return redirect(url_for('dashboard'))

    ids = {'ST': get_next_ticket_id('ST'), 'SW': get_next_ticket_id('SW'), 'LH': get_next_ticket_id('LH'), 'S3': get_next_ticket_id('S3')}
    return render_page(EXECUTION_PAGE, ids=ids)

@app.route('/cancel-job/<int:j_id>')
@login_required
def cancel_job(j_id):
    job = db.session.get(BatchJob, j_id)
    if not job:
        flash("Job not found", "danger")
        return redirect(url_for('dashboard'))

    if job.status == 'Scheduled':
        try:
            scheduler.remove_job(f"j_{job.id}")
            job.status = 'Cancelled'
            db.session.commit()
            flash(f"Job {job.ticket_id} schedule removed.", "success")
            log_activity(f"Cancelled schedule for {job.ticket_id}")
        except Exception as e:
            flash(f"Error removing schedule: {str(e)}", "warning")

    elif job.status == 'Running':
        if job.pid:
            try:
                os.kill(job.pid, signal.SIGTERM)
                job.status = 'Cancelled'
                job.completion_time = datetime.datetime.now()
                db.session.commit()
                flash(f"Job {job.ticket_id} process terminated.", "success")
                log_activity(f"Terminated running job {job.ticket_id}")
            except Exception as e:
                flash(f"Failed to kill process: {str(e)}", "danger")
        else:
            flash("Cannot cancel this job (PID not found).", "warning")
    else:
        flash("Job cannot be cancelled (already completed or failed).", "info")

    return redirect(url_for('dashboard'))

@app.route('/reschedule', methods=['POST'])
@login_required
@role_required(['Admin', 'Editor'])
def reschedule_job_route():
    ticket_id = request.form['ticket_id']
    new_sched_str = request.form['new_sched']
    job = BatchJob.query.filter_by(ticket_id=ticket_id).first()
    if not job or job.status not in ['Scheduled', 'Failed']:
        flash(f"Reschedule denied for {ticket_id}", "danger")
        return redirect(url_for('execution_page'))
    try:
        new_dt = datetime.datetime.fromisoformat(new_sched_str)
        try: scheduler.remove_job(f"j_{job.id}")
        except: pass

        mapping = {
            'studio': '/var/www/html/batch-web/scripts/Studio-Staging-Advanced.sh',
            'bollywood': '/var/www/html/batch-web/scripts/SW-Bollywood-Advanced.sh',
            'llh': '/var/www/html/batch-web/scripts/LLH-Upload-Advanced.sh',
            's3': '/var/www/html/batch-web/scripts/generic-s3.sh'
        }
        script = mapping.get(job.batch_type, '/opt/generic-s3.sh')
        cmd = [script, job.ticket_id, job.batch_number, 'default', 'cdnupdate@shemaroo.com']

        job.status, job.scheduled_time = 'Scheduled', new_dt
        db.session.commit()
        scheduler.add_job(id=f"j_{job.id}", func=run_script_task, trigger='date', run_date=new_dt, args=[job.id, cmd, job.log_file_path])
        flash(f"Job {ticket_id} updated", "success")
        log_activity(f"Rescheduled job {ticket_id}")
    except Exception as e: flash(str(e), "danger")
    return redirect(url_for('execution_page'))

@app.route('/backup-db', methods=['POST'])
@login_required
@role_required(['Admin'])
def backup_db():
    try:
        file = f"{BACKUP_DIR}/batch_system_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy(DB_FILE.replace('sqlite:///', ''), file)
        flash(f"Backup created: {file}", "success")
    except Exception as e: flash(f"Backup Error: {str(e)}", "danger")
    return redirect(url_for('admin_tools'))

@app.route('/restore-db', methods=['POST'])
@login_required
@role_required(['Admin'])
def restore_db():
    if 'db_file' in request.files:
        file = request.files['db_file']
        if file.filename.endswith('.db'):
            filepath = f"{BACKUP_DIR}/{file.filename}"
            file.save(filepath)
            try:
                shutil.copy(filepath, DB_FILE.replace('sqlite:///', ''))
                flash("Database restored successfully", "success")
            except Exception as e: flash(f"Restore failed: {e}", "danger")
    return redirect(url_for('admin_tools'))

@app.route('/job-log/<int:j_id>')
@login_required
def view_job_log(j_id):
    job = db.session.get(BatchJob, j_id)
    if job and os.path.exists(job.log_file_path):
        with open(job.log_file_path, 'r') as f: return f"<pre>{f.read()}</pre>"
    return "Log file not found."

@app.route('/user-mgmt')
@login_required
@role_required(['Admin'])
def user_mgmt():
    return render_page(USER_MGMT_PAGE, users=User.query.order_by(User.username).all())

@app.route('/user/create', methods=['POST'])
@login_required
@role_required(['Admin'])
def create_user():
    username = request.form['username'].strip()
    hashed = bcrypt.generate_password_hash(request.form['password']).decode('utf-8')
    db.session.add(User(username=username, password=hashed, role=request.form['role']))
    try:
        db.session.commit()
        flash("User created successfully", "success")
    except IntegrityError:
        db.session.rollback()
        flash(f"Username '{username}' already exists", "danger")
    return redirect(url_for('user_mgmt'))

@app.route('/user/update', methods=['POST'])
@login_required
@role_required(['Admin'])
def update_user():
    u = db.session.get(User, int(request.form['u_id']))
    if not u: return redirect(url_for('user_mgmt'))
    u.role = request.form['new_role']
    if request.form.get('new_pass'):
        u.password = bcrypt.generate_password_hash(request.form['new_pass']).decode('utf-8')
    if request.form.get('disable'):
        u.is_disabled = not u.is_disabled
    db.session.commit()
    flash(f"User {u.username} updated", "success")
    return redirect(url_for('user_mgmt'))

# ==========================================
# AUTO-MIGRATION & STARTUP
# ==========================================
def check_and_migrate_db():
    """Detects missing columns and adds them to avoid SQLite errors"""
    with app.app_context():
        inspector = inspect(db.engine)

        # User Table Migrations
        if 'user' in inspector.get_table_names():
            cols = [c['name'] for c in inspector.get_columns('user')]
            if 'last_activity' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE user ADD COLUMN last_activity DATETIME")); conn.commit()
            if 'last_ip' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE user ADD COLUMN last_ip VARCHAR(50)")); conn.commit()
            if 'last_hostname' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE user ADD COLUMN last_hostname VARCHAR(100)")); conn.commit()

        # BatchJob Table Migrations
        if 'batch_job' in inspector.get_table_names():
            cols = [c['name'] for c in inspector.get_columns('batch_job')]
            if 's3_link' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE batch_job ADD COLUMN s3_link VARCHAR(500)")); conn.commit()
            if 'estimated_time' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE batch_job ADD COLUMN estimated_time VARCHAR(50)")); conn.commit()
            if 'pid' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE batch_job ADD COLUMN pid INTEGER")); conn.commit()
            if 'source_path' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE batch_job ADD COLUMN source_path VARCHAR(300)")); conn.commit()
            if 'dest_path' not in cols:
                with db.engine.connect() as conn: conn.execute(text("ALTER TABLE batch_job ADD COLUMN dest_path VARCHAR(300)")); conn.commit()

        db.create_all() # Creates ArchiveData table if missing

if __name__ == '__main__':
    check_and_migrate_db()
    with app.app_context():
        if not User.query.filter_by(username='admin').first():
            db.session.add(User(username='admin', password=bcrypt.generate_password_hash('admin123').decode('utf-8'), role='Admin'))
            db.session.commit()

    if not scheduler.running:
        scheduler.init_app(app)
        scheduler.start()

    app.run(host='0.0.0.0', port=8080, debug=True)
