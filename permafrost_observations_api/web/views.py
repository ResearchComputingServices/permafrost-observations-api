#
# Main entry point for all views.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from flask import request, jsonify, url_for, Blueprint
from flask import json, jsonify, Response, blueprints
from permafrost_observations_api.web.common_view import permafrost_observations_bp
from permafrost_observations_api.decorators.crossorigin import crossdomain
from permafrost_observations_api.decorators.authorization import authorization
import permafrost_observations_api.web.location_of_observations_view
import permafrost_observations_api.web.download_observation_view

@permafrost_observations_bp.route("/", methods=['GET'])
@crossdomain(origin='*')
@authorization
def hello():
    return "Hello World!"

