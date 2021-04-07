#
# Specific view.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from flask import request, jsonify, abort,render_template,request,redirect,url_for, json, Response, send_file
from werkzeug import secure_filename
import os
from permafrost_observations_api.web.common_view import permafrost_observations_bp
from permafrost_observations_api.decorators.crossorigin import crossdomain
from permafrost_observations_api.decorators.authorization import authorization
from permafrost_observations_api.providers.raw_sql_provider import RawSqlProvider
from zipfile import ZipFile
from uuid import uuid4

provider = RawSqlProvider()

@permafrost_observations_bp.route("/download_observation_time_temperature", methods=['GET'])
@crossdomain(origin='*')
@authorization
def download_observation_time_temperature():
    location = request.args.get('location')
    if location:
        folder = 'download_observation_time_temperature'
        if not os.path.exists(folder):
            os.makedirs(folder)
        fullpath = os.path.join(folder, location + '.txt')
        provider.write_observation_time_temperature_into_file(fullpath, location)
        print(fullpath)
        return send_file(fullpath, as_attachment=True, cache_timeout=-1)
    return Response(json.dumps([]), 404, mimetype="application/json")

@permafrost_observations_bp.route("/download_observations_time_temperature", methods=['POST'])
@crossdomain(origin='*')
@authorization
def download_observation_time_temperatures():
    observations = request.get_json()
    if observations and len(observations) > 0:
        # Check if invalid data is being sent from the front-end.
        for observation in observations:
            if not isinstance(observation, str):
                return Response(json.dumps(observations), 400, mimetype="application/json")
        # If checks pass create the folder if it doesn't exist.
        folder = 'download_observation_time_temperature'
        if not os.path.exists(folder):
            os.makedirs(folder)
        # Generate a zip file path name dynamically.
        zip_path = os.path.join(folder, str(uuid4()) + '.zip')
        # Create the zip file and append all the observations into it.
        with ZipFile(zip_path, 'w') as newzip:
            for observation in observations:
                observation_file_path = os.path.join(folder, observation + '.txt')
                provider.write_observation_time_temperature_into_file(observation_file_path, observation)
                newzip.write(observation_file_path)
        # Send the zip file that got created back to the client.
        return send_file(zip_path, attachment_filename='observations.zip', as_attachment=True, cache_timeout=-1)
    return Response(json.dumps(observations), 404, mimetype="application/json")

@permafrost_observations_bp.route("/download_observation_temperature_height", methods=['GET'])
@crossdomain(origin='*')
@authorization
def download_observation_temperature_height():
    location = request.args.get('location')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if location:
        folder = 'download_observation_temperature_height'
        if not os.path.exists(folder):
            os.makedirs(folder)
        fullpath = os.path.join(folder, location + '.txt')
        provider.write_observation_temperature_height_into_file(fullpath, location, start_date, end_date)
        return send_file(fullpath, as_attachment=True, cache_timeout=-1)
    return Response(json.dumps([]), 404, mimetype="application/json")
