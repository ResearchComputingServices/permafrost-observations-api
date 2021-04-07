#
# Factory that maps Flask to physical database.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import json
from flask import jsonify
from werkzeug.routing import RequestRedirect
from permafrost_observations_api.decorators.crossorigin import crossdomain

def create_app(package_name):
    from flask import Flask
    app = Flask(package_name)
    from permafrost_observations_api.extensions import db, ma, migrate, oidc

    with open('client_secrets.json') as client_secrets_file:
        client_secrets = json.load(client_secrets_file)

    app.config['SQLALCHEMY_DATABASE_URI'] = client_secrets.get('postgres').get('database_uri')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    app.config.update({
        'SECRET_KEY': client_secrets.get('web').get('client_secret'),
        'TESTING': True,
        'DEBUG': True,
        'OIDC_OPENID_REALM': client_secrets.get('web').get('realm'),
        'OIDC_CLIENT_SECRETS': 'client_secrets.json',
        'OIDC_INTROSPECTION_AUTH_METHOD': 'client_secret_post',
        'OIDC_TOKEN_TYPE_HINT': 'access_token',
        'OIDC_ID_TOKEN_COOKIE_SECURE': False,
        'OIDC_REQUIRE_VERIFIED_EMAIL': False
    })

    db.init_app(app)
    ma.init_app(app)
    migrate.init_app(app, db)
    oidc.init_app(app)

    @app.route('/favicon.ico')
    def favi():
        return 'Hello FaviTown', 200

    return app

def register_blueprints(app):
    from permafrost_observations_api.web.views import permafrost_observations_bp
    app.register_blueprint(permafrost_observations_bp, url_prefix='/permafrost_observations_api')
    return app

def register_error_handlers(app):
    @app.errorhandler(AssertionError)
    @crossdomain(origin='*')
    def assertion_exception_handler(error):
        """
        Intercepts Assertions Errors that bubble passed the view so as to not kill the app unnecessarily
        Mainly user error hence the 400
        :param error: The assertion error being raised
        :return: A str representation of the error and a 400
        """
        out = {
            'error': {
                'code': str(type(error)),
                'message': str(error)
            }
        }
        return jsonify(out), 400

    @app.errorhandler(Exception)
    @crossdomain(origin='*')
    def exception_handler(error):
        """
            Intercepts Exceptions that bubble passed the view so as to not kill the app unnecessarily
            Most likely a server error so returns 500
            :param error: The error being raised
            :return: A str representation of the error and a 500
            """
        out = {
            'error': {
                'code': str(type(error)),
                'message': str(error)
            }
        }
        print(error)
        return jsonify(out), 500

    @app.errorhandler(RequestRedirect)
    @crossdomain(origin='*')
    def request_redirect(error):
        return error

