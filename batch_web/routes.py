import datetime
import json
import os
import shutil
import signal
import subprocess
import threading

from flask import flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required, login_user, logout_user
from sqlalchemy.exc import IntegrityError

from .config import BACKUP_DIR, DB_FILE, JOB_LOG_DIR, WEB_LOG, ARCHIVE_DIR
from .extensions import bcrypt, db, scheduler
from .models import ArchiveData, AwsCredential, BatchJob, User, UserActivity
from .services import create_s3_links_from_batch_files, get_client_info, get_next_ticket_id, log_activity, role_required, verify_nas_path, verify_s3_access
from .tasks import run_script_task, run_universal_transfer_task


def register_routes(app):
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
        return render_template('login.html')

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

        return render_template('dashboard.html', history=history, active_users=active_users,
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
            if request.form.get('cred_action') == 'create':
                cred = AwsCredential(
                    vendor_name=request.form['vendor_name'].strip(),
                    aws_profile=request.form['aws_profile'].strip(),
                    access_key=request.form.get('access_key', '').strip() or None,
                    secret_key=request.form.get('secret_key', '').strip() or None,
                    region=request.form.get('region', '').strip() or None,
                    notes=request.form.get('notes', '').strip() or None,
                    is_active=True,
                )
                db.session.add(cred)
                try:
                    db.session.commit()
                    log_activity(f"Created AWS credential vendor={cred.vendor_name}, profile={cred.aws_profile}")
                    flash("AWS credential created", "success")
                except IntegrityError:
                    db.session.rollback()
                    flash("Vendor name already exists", "danger")
            elif request.form.get('cred_action') == 'update':
                cred = db.session.get(AwsCredential, int(request.form['cred_id']))
                if cred:
                    cred.vendor_name = request.form['vendor_name'].strip()
                    cred.aws_profile = request.form['aws_profile'].strip()
                    cred.access_key = request.form.get('access_key', '').strip() or None
                    cred.secret_key = request.form.get('secret_key', '').strip() or None
                    cred.region = request.form.get('region', '').strip() or None
                    cred.notes = request.form.get('notes', '').strip() or None
                    cred.is_active = bool(request.form.get('is_active'))
                    db.session.commit()
                    log_activity(f"Updated AWS credential vendor={cred.vendor_name}, profile={cred.aws_profile}")
                    flash("AWS credential updated", "success")
            elif 'audit_action' in request.form:
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

        aws_credentials = AwsCredential.query.order_by(AwsCredential.vendor_name).all()
        return render_template('admin_tools.html', history=history, activities=activities,
                           backup_file=backup_file, audit_s3=s3_list, audit_nas=nas_list,
                           s3_links=s3_links_result, web_log_tail=web_log_tail,
                           aws_credentials=aws_credentials)

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

    @app.route('/verify/nas', methods=['POST'])
    @login_required
    def verify_nas_route():
        payload = request.get_json(silent=True) or {}
        nas_path = payload.get('nas_path', '').strip()
        ok, msg = verify_nas_path(nas_path)
        log_activity(f"Verify NAS path={nas_path}, success={ok}")
        return jsonify(success=ok, message=msg)

    @app.route('/verify/s3', methods=['POST'])
    @login_required
    def verify_s3_route():
        payload = request.get_json(silent=True) or {}
        s3_path = payload.get('s3_path', '').strip()
        vendor_id = payload.get('vendor_id')
        cred = db.session.get(AwsCredential, int(vendor_id)) if vendor_id else None
        if not cred:
            return jsonify(success=False, message="Invalid vendor credential")
        ok, msg = verify_s3_access(s3_path, cred.aws_profile)
        log_activity(f"Verify S3 path={s3_path}, vendor={cred.vendor_name}, success={ok}")
        return jsonify(success=ok, message=msg)

    @app.route('/execution', methods=['GET', 'POST'])
    @login_required
    def execution_page():
        if request.method == 'POST':
            panel = request.form['panel']
            mapping = {
                'studio': ('ST', '/var/www/html/batch-web/scripts/Studio-Staging-Advanced.sh'),
                'bollywood': ('SW', '/var/www/html/batch-web/scripts/SW-Bollywood-Advanced.sh'),
                'llh': ('LH', '/var/www/html/batch-web/scripts/LLH-Upload-Advanced.sh'),
                's3': ('S3', '/var/www/html/batch-web/scripts/generic-s3.sh'),
                'universal_upload': ('UA', None),
                'universal_download': ('UD', None),
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
                new_j = BatchJob(source_path=src, dest_path=dst, aws_profile=request.form['aws'])
            elif panel in ['universal_upload', 'universal_download']:
                cred = db.session.get(AwsCredential, int(request.form['vendor_id']))
                if not cred or not cred.is_active:
                    flash("Selected vendor credential is invalid or inactive", "danger")
                    return redirect(url_for('execution_page'))
                nas_path = request.form['nas_path'].strip()
                s3_path = request.form['s3_path'].strip()
                nas_ok, nas_msg = verify_nas_path(nas_path)
                s3_ok, s3_msg = verify_s3_access(s3_path, cred.aws_profile)
                if not nas_ok or not s3_ok:
                    flash("Universal transfer verification failed. Check popup verification details before submit.", "danger")
                    log_activity(f"Universal verify failed ticket={t_id}, vendor={cred.vendor_name}, nas_ok={nas_ok}, s3_ok={s3_ok}")
                    return redirect(url_for('execution_page'))
                b_no = "CUSTOM"
                cmd = None
                new_j = BatchJob(source_path=nas_path, dest_path=s3_path, vendor_name=cred.vendor_name, aws_profile=cred.aws_profile)
                log_activity(f"Universal request: panel={panel}, ticket={t_id}, vendor={cred.vendor_name}, nas={nas_path}, s3={s3_path}")
            else:
                b_no = request.form['b_no']
                cmd = [script, t_id, b_no, request.form['aws'], request.form['rcp']]
                new_j = BatchJob(aws_profile=request.form['aws'])

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
                if panel in ['universal_upload', 'universal_download']:
                    mode = 'upload' if panel == 'universal_upload' else 'download'
                    scheduler.add_job(id=f"j_{new_j.id}", func=run_universal_transfer_task, trigger='date', run_date=dt, args=[new_j.id, mode, new_j.source_path, new_j.dest_path, new_j.aws_profile, log_f])
                else:
                    scheduler.add_job(id=f"j_{new_j.id}", func=run_script_task, trigger='date', run_date=dt, args=[new_j.id, cmd, log_f])
                flash(f"Job {t_id} scheduled for {sched_val}", "info")
                log_activity(f"Scheduled job {t_id}")
            else:
                new_j.status = 'Running'
                db.session.add(new_j); db.session.commit()
                if panel in ['universal_upload', 'universal_download']:
                    mode = 'upload' if panel == 'universal_upload' else 'download'
                    threading.Thread(target=run_universal_transfer_task, args=(new_j.id, mode, new_j.source_path, new_j.dest_path, new_j.aws_profile, log_f)).start()
                else:
                    threading.Thread(target=run_script_task, args=(new_j.id, cmd, log_f)).start()
                flash(f"Job {t_id} started immediately", "success")
                log_activity(f"Started job {t_id}")
            return redirect(url_for('dashboard'))

        ids = {'ST': get_next_ticket_id('ST'), 'SW': get_next_ticket_id('SW'), 'LH': get_next_ticket_id('LH'), 'S3': get_next_ticket_id('S3'),
               'UA': get_next_ticket_id('UA'), 'UD': get_next_ticket_id('UD')}
        aws_credentials = AwsCredential.query.filter_by(is_active=True).order_by(AwsCredential.vendor_name).all()
        return render_template('execution.html', ids=ids, aws_credentials=aws_credentials)

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
                's3': '/var/www/html/batch-web/scripts/generic-s3.sh',
            }
            script = mapping.get(job.batch_type, '/opt/generic-s3.sh')
            cmd = [script, job.ticket_id, job.batch_number, 'default', 'cdnupdate@shemaroo.com']

            job.status, job.scheduled_time = 'Scheduled', new_dt
            db.session.commit()
            if job.batch_type in ['universal_upload', 'universal_download']:
                mode = 'upload' if job.batch_type == 'universal_upload' else 'download'
                scheduler.add_job(id=f"j_{job.id}", func=run_universal_transfer_task, trigger='date', run_date=new_dt, args=[job.id, mode, job.source_path, job.dest_path, job.aws_profile, job.log_file_path])
            else:
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
        return render_template('user_mgmt.html', users=User.query.order_by(User.username).all())

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
