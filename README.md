# Batch Upload Web Portal

A Flask-based operations portal for batch uploads, scheduled jobs, NAS/S3 validation, AWS vendor profile management, and audit logging.

## Project Layout

```text
Batch-Upload/
├── app.py                    # Local development entrypoint
├── wsgi.py                   # Production WSGI entrypoint
├── requirements.txt          # Python runtime dependencies
├── batch_web/
│   ├── app_factory.py        # Flask application factory and runtime initialization
│   ├── config.py             # Paths, database URI, and directory creation
│   ├── extensions.py         # Flask extension singletons
│   ├── migrations.py         # Lightweight SQLite auto-migration helpers
│   ├── models.py             # SQLAlchemy models
│   ├── routes.py             # HTTP routes and page/API handlers
│   ├── services.py           # Shared business logic and validation helpers
│   └── tasks.py              # Background/scheduled job execution logic
└── templates/
    ├── base.html             # Shared layout/sidebar
    ├── login.html            # Login page
    ├── dashboard.html        # Dashboard page
    ├── execution.html        # Execution and Universal Upload/Download page
    ├── admin_tools.html      # Admin tools and AWS credential panel
    └── user_mgmt.html        # User management page
```

## Features

- User login/logout with role-based Admin/Editor/Viewer access control.
- Batch execution, scheduling, cancellation, and job log viewing.
- Admin audit tools for NAS/S3 path checks and S3 link generation.
- AWS credential list panel for vendor profile records.
- Universal Upload flow: NAS UNC path to S3 destination.
- Universal Download flow: S3 source to NAS UNC path.
- Verification APIs for NAS content listing and S3 list/upload/delete permissions.
- SQLite auto-migration for newly introduced columns and tables.
- Web/activity logging under the configured batch web log directory.

## Runtime Requirements

- Python 3.10+ recommended.
- AWS CLI installed at `/usr/local/bin/aws`.
- Linux CIFS mount support (`mount -t cifs`) for UNC NAS verification/transfer.
- Server permissions to mount/unmount temporary NAS mount points.
- AWS CLI profiles already configured on the host for the profile names stored in the AWS credential list.

## Configuration

The application defaults to the existing production path `/var/www/html/batch-web`.
You can override paths and secrets with environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BATCH_WEB_BASE_DIR` | `/var/www/html/batch-web` | Root for logs, backups, and DB file |
| `BATCH_WEB_DB_PATH` | `$BATCH_WEB_BASE_DIR/batch_system.db` | SQLite database file path |
| `BATCH_WEB_SECRET_KEY` | `GlobalBatchKey2026` | Flask session secret |

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BATCH_WEB_BASE_DIR=/tmp/batch-web-dev
python app.py
```

The app starts on `http://0.0.0.0:8080` and creates a default Admin user when missing:

- Username: `admin`
- Password: `admin123`

Change the default password before production use.

## Production Deployment

Install dependencies and run the WSGI entrypoint from a process manager such as systemd + gunicorn:

```bash
pip install -r requirements.txt
gunicorn --workers 2 --bind 0.0.0.0:8080 wsgi:app
```

The `wsgi.py` entrypoint initializes database migrations, the default admin user, and the scheduler before serving traffic.

## Universal Transfer Workflow

1. Admin creates or updates a vendor AWS profile in **Admin Tools → AWS Credential List**.
2. User opens **Execution** and chooses **Universal Upload** or **Universal Download**.
3. User selects vendor, enters NAS/S3 paths, and clicks the verify buttons.
4. The server verifies:
   - NAS path by temporarily mounting the UNC share and listing contents.
   - S3 path by running AWS CLI list, test upload, and cleanup commands.
5. If verification passes, the job can run immediately or be scheduled.
6. Job progress is streamed to the per-job log and visible from dashboard/history log buttons.

## Validation

Use this compile check after code changes:

```bash
python -m py_compile app.py batch_web/*.py
```
