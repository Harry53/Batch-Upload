import datetime

from flask_login import UserMixin

from .extensions import db


class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)
    role = db.Column(db.String(20), default='Viewer')
    is_online = db.Column(db.Boolean, default=False)
    is_disabled = db.Column(db.Boolean, default=False)
    last_login = db.Column(db.DateTime, nullable=True)
    last_activity = db.Column(db.DateTime, default=datetime.datetime.now)
    last_ip = db.Column(db.String(50), nullable=True)
    last_hostname = db.Column(db.String(100), nullable=True)


class BatchJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.String(50))
    batch_type = db.Column(db.String(50))
    batch_number = db.Column(db.String(50))
    status = db.Column(db.String(20))
    log_file_path = db.Column(db.String(200))
    triggered_by = db.Column(db.String(50))
    start_time = db.Column(db.DateTime, default=datetime.datetime.now)
    scheduled_time = db.Column(db.DateTime, nullable=True)
    completion_time = db.Column(db.DateTime, nullable=True)
    s3_link = db.Column(db.String(500), nullable=True)
    estimated_time = db.Column(db.String(50), nullable=True)
    pid = db.Column(db.Integer, nullable=True)
    source_path = db.Column(db.String(300), nullable=True)
    dest_path = db.Column(db.String(300), nullable=True)
    vendor_name = db.Column(db.String(120), nullable=True)
    aws_profile = db.Column(db.String(120), nullable=True)


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
    job_data = db.Column(db.Text)
    activity_data = db.Column(db.Text)


class AwsCredential(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vendor_name = db.Column(db.String(120), unique=True, nullable=False)
    aws_profile = db.Column(db.String(120), nullable=False)
    access_key = db.Column(db.String(200), nullable=True)
    secret_key = db.Column(db.String(200), nullable=True)
    region = db.Column(db.String(50), nullable=True)
    notes = db.Column(db.String(255), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
