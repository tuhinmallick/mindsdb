from http import HTTPStatus

from flask_restx import Resource, abort
from sqlalchemy.exc import NoResultFound


from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController


@ns_conf.route('/')
class ProjectsList(Resource):
    @ns_conf.doc('list_projects')
    def get(self):
        ''' List all projects '''
        session = SessionController()

        return [{'name': i} for i in session.datahub.get_projects_names()]


@ns_conf.route('/<project_name>')
class ProjectsGet(Resource):
    @ns_conf.doc('get_project')
    def get(self, project_name):
        '''Gets a project by name'''
        session = SessionController()

        try:
            project = session.database_controller.get_project(project_name)
        except NoResultFound:
            abort(HTTPStatus.NOT_FOUND, f'Project name {project_name} does not exist')

        return {
            'name': project.name
        }
