import os
import copy
import shutil
import datetime
import tempfile
from pathlib import Path
from dateutil.parser import parse as parse_datetime

from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from dateutil.tz import tzlocal

from mindsdb.utilities import log
from mindsdb.utilities.functions import encrypt, decrypt
from mindsdb.api.http.namespaces.configs.config import ns_conf
from mindsdb.utilities.log_controller import get_logs
from mindsdb.utilities.config import Config
from mindsdb.api.http.utils import http_error


@ns_conf.route('/logs')
@ns_conf.param('name', 'Get logs')
class GetLogs(Resource):
    @ns_conf.doc('get_integrations')
    def get(self):
        min_timestamp = parse_datetime(request.args['min_timestamp'])
        max_timestamp = request.args.get('max_timestamp', None)
        context = request.args.get('context', None)
        level = request.args.get('level', None)
        log_from = request.args.get('log_from', None)
        limit = request.args.get('limit', None)

        logs = get_logs(min_timestamp, max_timestamp, context, level, log_from, limit)
        return {'data': logs}


@ns_conf.route('/')
@ns_conf.param('name', 'Get config')
class GetConfig(Resource):
    @ns_conf.doc('get_config')
    def get(self):
        config = Config()
        return {
            'auth': {
                'http_auth_enabled': config['auth']['http_auth_enabled'],
                'username': config['auth']['username'],
                'password': config['auth']['password']
            }
        }

    @ns_conf.doc('put_config')
    def put(self):
        data = request.json

        if unknown_argumens := list(set(data.keys()) - {'auth'}):
            return http_error(
                400, 'Wrong arguments',
                f'Unknown argumens: {unknown_argumens}'
            )

        for key in data.keys():
            if unknown_argumens := list(
                set(data[key].keys()) - set(Config()[key].keys())
            ):
                return http_error(
                    400, 'Wrong arguments',
                    f'Unknown argumens: {unknown_argumens}'
                )

        Config().update(data)

        return '', 200


@ns_conf.route('/integrations')
@ns_conf.param('name', 'List all database integration')
class ListIntegration(Resource):
    def get(self):
        return {
            'integrations': list(
                ca.integration_controller.get_all(sensitive_info=False)
            )
        }


@ns_conf.route('/all_integrations')
@ns_conf.param('name', 'List all database integration')
class AllIntegration(Resource):
    @ns_conf.doc('get_all_integrations')
    def get(self):
        return ca.integration_controller.get_all(sensitive_info=False)


@ns_conf.route('/integrations/<name>')
@ns_conf.param('name', 'Database integration')
class Integration(Resource):
    @ns_conf.doc('get_integration')
    def get(self, name):
        integration = ca.integration_controller.get(name, sensitive_info=False)
        if integration is None:
            abort(404, f'Can\'t find database integration: {name}')
        integration = copy.deepcopy(integration)
        return integration

    @ns_conf.doc('put_integration')
    def put(self, name):
        params = {}
        params.update((request.json or {}).get('params', {}))
        params.update(request.form or {})

        if not params:
            abort(400, "type of 'params' must be dict")

        files = request.files
        temp_dir = None
        if files is not None and len(files) > 0:
            temp_dir = tempfile.mkdtemp(prefix='integration_files_')
            for key, file in files.items():
                temp_dir_path = Path(temp_dir)
                file_name = Path(file.filename)
                file_path = temp_dir_path.joinpath(file_name).resolve()
                if temp_dir_path not in file_path.parents:
                    raise Exception(f'Can not save file at path: {file_path}')
                file.save(file_path)
                params[key] = file_path

        is_test = params.get('test', False)

        config = Config()
        secret_key = config.get('secret_key', 'dummy-key')

        if is_test:
            del params['test']

            handler_type = params.pop('type', None)
            params.pop('publish', None)
            handler = ca.integration_controller.create_tmp_handler(
                handler_type=handler_type,
                connection_data=params
            )

            status = handler.check_connection()
            if temp_dir is not None:
                shutil.rmtree(temp_dir)

            resp = status.to_json()
            if status.success and 'code' in params:
                if hasattr(handler, 'handler_storage'):
                    if export := handler.handler_storage.export_files():
                        # encrypt with flask secret key
                        encrypted = encrypt(export, secret_key)
                        resp['storage'] = encrypted.decode()

            return resp, 200

        integration = ca.integration_controller.get(name, sensitive_info=False)
        if integration is not None:
            abort(400, f"Integration with name '{name}' already exists")

        try:
            engine = params['type']
            if engine is not None:
                del params['type']
            params.pop('publish', False)
            storage = params.pop('storage', None)
            ca.integration_controller.add(name, engine, params)

            # copy storage
            if storage is not None:
                handler = ca.integration_controller.get_handler(name)

                export = decrypt(storage.encode(), secret_key)
                handler.handler_storage.import_files(export)

        except Exception as e:
            log.logger.error(str(e))
            if temp_dir is not None:
                shutil.rmtree(temp_dir)
            abort(500, f'Error during config update: {str(e)}')

        if temp_dir is not None:
            shutil.rmtree(temp_dir)
        return {}, 200

    @ns_conf.doc('delete_integration')
    def delete(self, name):
        integration = ca.integration_controller.get(name)
        if integration is None:
            abort(400, f"Nothing to delete. '{name}' not exists.")
        try:
            ca.integration_controller.delete(name)
        except Exception as e:
            log.logger.error(str(e))
            abort(500, f'Error during integration delete: {str(e)}')
        return '', 200

    @ns_conf.doc('modify_integration')
    def post(self, name):
        params = {}
        params.update((request.json or {}).get('params', {}))
        params.update(request.form or {})

        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        integration = ca.integration_controller.get(name)
        if integration is None:
            abort(400, f"Nothin to modify. '{name}' not exists.")
        try:
            if 'enabled' in params:
                params['publish'] = params['enabled']
                del params['enabled']
            ca.integration_controller.modify(name, params)

        except Exception as e:
            log.logger.error(str(e))
            abort(500, f'Error during integration modifycation: {str(e)}')
        return '', 200


@ns_conf.route('/integrations/<name>/check')
@ns_conf.param('name', 'Database integration checks')
class Check(Resource):
    @ns_conf.doc('check')
    def get(self, name):
        if ca.integration_controller.get(name) is None:
            abort(404, f'Can\'t find database integration: {name}')
        connections = ca.integration_controller.check_connections()
        return connections.get(name, False), 200


@ns_conf.route('/vars')
class Vars(Resource):
    def get(self):
        telemtry = os.getenv('CHECK_FOR_UPDATES', '1').lower() not in ['0', 'false']
        mongo = not ca.config_obj.get('disable_mongo', False)
        cloud = ca.config_obj.get('cloud', False)
        local_time = datetime.datetime.now(tzlocal())
        local_timezone = local_time.tzname()

        return {
            'mongo': mongo,
            'telemtry': telemtry,
            'cloud': cloud,
            'timezone': local_timezone,
        }
