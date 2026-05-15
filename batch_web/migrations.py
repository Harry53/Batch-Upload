from sqlalchemy import inspect, text

from .extensions import db


def check_and_migrate_db(app):
    """Detect missing columns and add them so older SQLite DBs keep working."""
    with app.app_context():
        inspector = inspect(db.engine)

        if 'user' in inspector.get_table_names():
            cols = [c['name'] for c in inspector.get_columns('user')]
            if 'last_activity' not in cols:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE user ADD COLUMN last_activity DATETIME")); conn.commit()
            if 'last_ip' not in cols:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE user ADD COLUMN last_ip VARCHAR(50)")); conn.commit()
            if 'last_hostname' not in cols:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE user ADD COLUMN last_hostname VARCHAR(100)")); conn.commit()

        if 'batch_job' in inspector.get_table_names():
            cols = [c['name'] for c in inspector.get_columns('batch_job')]
            migrations = {
                's3_link': "ALTER TABLE batch_job ADD COLUMN s3_link VARCHAR(500)",
                'estimated_time': "ALTER TABLE batch_job ADD COLUMN estimated_time VARCHAR(50)",
                'pid': "ALTER TABLE batch_job ADD COLUMN pid INTEGER",
                'source_path': "ALTER TABLE batch_job ADD COLUMN source_path VARCHAR(300)",
                'dest_path': "ALTER TABLE batch_job ADD COLUMN dest_path VARCHAR(300)",
                'vendor_name': "ALTER TABLE batch_job ADD COLUMN vendor_name VARCHAR(120)",
                'aws_profile': "ALTER TABLE batch_job ADD COLUMN aws_profile VARCHAR(120)",
            }
            for column, sql in migrations.items():
                if column not in cols:
                    with db.engine.connect() as conn:
                        conn.execute(text(sql)); conn.commit()

        db.create_all()
