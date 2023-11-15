import logging
import os
try:
    import torch.multiprocessing as mp
except Exception:
    import multiprocessing as mp

from waitress import serve

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache


def start(verbose, no_studio):
    config = Config()

    server = os.environ.get('MINDSDB_DEFAULT_SERVER', 'waitress')
    db.init()
    log.initialize_log(config, 'http', wrap_print=server.lower() != 'gunicorn')

    init_lexer_parsers()

    app = initialize_app(config, no_studio)

    port = config['api']['http']['port']
    host = config['api']['http']['host']

    process_cache.init()

    if server.lower() == 'waitress':
        serve(
            app,
            host='*' if host in ('', '0.0.0.0') else host,
            port=port,
            threads=16,
            max_request_body_size=1073741824 * 10,
            inbuf_overflow=1073741824 * 10
        )
    elif server.lower() == 'flask':
        # that will 'disable access' log in console
        logger = logging.getLogger('werkzeug')
        logger.setLevel(logging.WARNING)

        app.run(debug=False, port=port, host=host)
    elif server.lower() == 'gunicorn':
        try:
            from mindsdb.api.http.gunicorn_wrapper import StandaloneApplication
        except ImportError:
            print("Gunicorn server is not available by default. If you wish to use it, please install 'gunicorn'")
            return

        def post_fork(arbiter, worker):
            db.engine.dispose()

        options = {
            'bind': f'{host}:{port}',
            'workers': mp.cpu_count(),
            'timeout': 600,
            'reuse_port': True,
            'preload_app': True,
            'post_fork': post_fork,
            'threads': 4
        }
        StandaloneApplication(app, options).run()
