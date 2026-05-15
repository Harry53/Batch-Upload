from batch_web import create_app
from batch_web.app_factory import initialize_runtime

app = create_app()

if __name__ == '__main__':
    initialize_runtime(app)
    app.run(host='0.0.0.0', port=8080, debug=True)
