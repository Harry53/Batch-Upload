from flask import Flask

from .config import DB_FILE, SECRET_KEY, ensure_directories
from .extensions import bcrypt, db, login_manager, scheduler
from .migrations import check_and_migrate_db
from .models import User
from .routes import register_routes
from .tasks import init_task_app


def create_app():
    ensure_directories()
    app = Flask(__name__, template_folder='../templates')
    app.config['SECRET_KEY'] = SECRET_KEY
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_FILE
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    scheduler.init_app(app)
    init_task_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        user = db.session.get(User, int(user_id))
        if user and user.is_disabled:
            return None
        return user

    register_routes(app)
    return app


def initialize_runtime(app):
    check_and_migrate_db(app)
    with app.app_context():
        if not User.query.filter_by(username='admin').first():
            db.session.add(User(username='admin', password=bcrypt.generate_password_hash('admin123').decode('utf-8'), role='Admin'))
            db.session.commit()
    if not scheduler.running:
        scheduler.start()
