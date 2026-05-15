from batch_web import create_app
from batch_web.app_factory import initialize_runtime

app = create_app()
initialize_runtime(app)
