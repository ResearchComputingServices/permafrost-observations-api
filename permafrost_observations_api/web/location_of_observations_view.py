#
# Specific view.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import math
from decimal import *
from flask import json, jsonify, Response, blueprints, request
from permafrost_observations_api.web.common_view import permafrost_observations_bp
from permafrost_observations_api.decorators.crossorigin import crossdomain
from permafrost_observations_api.decorators.authorization import authorization
from permafrost_observations_api.providers.raw_sql_provider import RawSqlProvider

provider = RawSqlProvider()

def get_name_pattern():
    name_pattern = request.args.get('name_pattern')
    if name_pattern:
        name_pattern = name_pattern.replace("*", "%")
    name_pattern = name_pattern if name_pattern else '%'
    return name_pattern

@permafrost_observations_bp.route("/locations_of_observations")
@crossdomain(origin='*')
@authorization
def get_locations_of_observations():
    geometry_type = request.args.get('geometry_type')
    geometry_type = geometry_type if geometry_type else 'ST_Point'
    name_pattern = get_name_pattern()

    sql_statement = """SELECT name, 
                              ST_X(coordinates) AS lon, 
                              ST_Y(coordinates) AS lat,
                              elevation_in_metres,
                              comment,
                              record_observations,
                              accuracy_in_metres,
                              'Carleton Internal' as provider
                       FROM LOCATIONS
                       WHERE name LIKE :name_pattern AND ST_GeometryType(coordinates)=:geometry_type
                       ORDER BY name ASC"""
    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'geometry_type': geometry_type, 'name_pattern': name_pattern })
    return provider.jsonify_records(records)

@permafrost_observations_bp.route("/locations_of_observations_as_markers")
@crossdomain(origin='*')
@authorization
def get_locations_of_observations_as_markers():
    geometry_type = request.args.get('geometry_type')
    geometry_type = geometry_type if geometry_type else 'ST_Point'
    name_pattern = get_name_pattern()

    sql_statement = """SELECT name, 
                              ST_X(coordinates) AS lon, 
                              ST_Y(coordinates) AS lat,
                              elevation_in_metres,
                              comment,
                              record_observations,
                              accuracy_in_metres,
                              'Carleton Internal' as provider
                       FROM LOCATIONS
                       WHERE name LIKE :name_pattern AND ST_GeometryType(coordinates)=:geometry_type
                       ORDER BY name ASC"""
    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'geometry_type': geometry_type, 'name_pattern': name_pattern })
    return provider.jsonify_records(records)

@permafrost_observations_bp.route("/locations_of_observations/count")
@crossdomain(origin='*')
@authorization
def get_locations_of_observations_count():
    geometry_type = request.args.get('geometry_type')
    geometry_type = geometry_type if geometry_type else 'ST_Point'
    name_pattern = get_name_pattern()

    sql_statement = """SELECT name, ST_X(coordinates) AS lon, ST_Y(coordinates) AS lat
                       FROM LOCATIONS
                       WHERE name LIKE :name_pattern AND ST_GeometryType(coordinates)=:geometry_type
                       ORDER BY name ASC"""
    sql_statement = provider.apply_limit_and_offset(sql_statement)
    sql_statement = provider.apply_count(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'geometry_type': geometry_type, 'name_pattern': name_pattern })
    array = []
    for record in records:
        item = {}
        item["count"] = record.count
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/locations_of_observations_as_markers", methods=['POST'])
@crossdomain(origin='*')
@authorization
def add_locations_of_observations():
    # Insert a new location
    data = request.get_json()
    name = data.get('text')
    lat = data.get('lat')
    lng = data.get('lng')
    comment = data.get('comment')
    accuracy_in_metres = data.get('accuracy_in_metres')
    elevation_in_metres = data.get('elevation_in_metres')
    record_observations = data.get('record_observations')

    sql_statement = """INSERT into locations (name, coordinates, accuracy_in_metres, comment, record_observations,
                                              elevation_in_metres) 
                       VALUES(:name, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326), 
                              :accuracy_in_metres, :comment, :record_observations, :elevation_in_metres)"""
    params = {
        'name': name,
        'lat': lat,
        'lng': lng,
        'comment': comment,
        'record_observations': record_observations,
        'accuracy_in_metres': accuracy_in_metres,
        'elevation_in_metres': elevation_in_metres
    }
    records = provider.execute_sql(sql_statement, params)
    return jsonify([])

@permafrost_observations_bp.route("/locations_of_observations_as_markers", methods=['PUT'])
@crossdomain(origin='*')
@authorization
def update_locations_of_observations():
    # Update an existing location
    data = request.get_json()
    name = data.get('text')
    lat = data.get('lat')
    lng = data.get('lng')
    comment = data.get('comment')
    accuracy_in_metres = data.get('accuracy_in_metres')
    elevation_in_metres = data.get('elevation_in_metres')
    record_observations = data.get('record_observations')

    sql_statement = """UPDATE locations 
                       SET coordinates = ST_SetSRID(ST_MakePoint(:lng, :lat), 4326),
                           accuracy_in_metres = :accuracy_in_metres,
                           comment = :comment,
                           record_observations = :record_observations,
                           elevation_in_metres = :elevation_in_metres
                       WHERE name = :name"""
    params = {
        'name': name,
        'lat': lat,
        'lng': lng,
        'comment': comment,
        'record_observations': record_observations,
        'accuracy_in_metres': accuracy_in_metres,
        'elevation_in_metres': elevation_in_metres
    }
    records = provider.execute_sql(sql_statement, params)
    return jsonify([])

@permafrost_observations_bp.route("/locations_of_observations_as_markers", methods=['DELETE'])
@crossdomain(origin='*')
@authorization
def delete_locations_of_observations():
    # Delete a location
    data = request.get_json()
    name = data.get('text')

    sql_statement = """DELETE FROM locations
                       WHERE name = :name"""
    params = {
        'name': name
    }
    records = provider.execute_sql(sql_statement, params)
    return jsonify([])

@permafrost_observations_bp.route("/ground_temperatures")
@crossdomain(origin='*')
@authorization
def get_ground_temperatures():
    location = request.args.get('location')

    sql_statement = """SELECT locations.name AS loc_name, 
                            observations.height_min_metres AS height, 
                            AVG(observations.numeric_value) as agg_avg, 
                            COUNT(observations.numeric_value) as agg_cnt, 
                            TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM observations.corrected_utc_time) / 86400) * 86400) AT TIME ZONE 'UTC' AS time 
                       FROM observations 
                            INNER JOIN locations ON observations.location = locations.coordinates 
                       WHERE observations.corrected_utc_time BETWEEN '1950-01-01 00:00:00+00' AND '2050-01-01 00:00:00+00' 
                            AND locations.name = :location 
                            AND observations.unit_of_measure = 'C' 
                            GROUP BY observations.height_min_metres, locations.name, time 
                            ORDER BY loc_name ASC, height DESC """
    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'location': location })
    array = []
    for record in records:
        item = {}
        item["loc_name"] = record.loc_name
        item["height"] = float(record.height)
        item["agg_avg"] = float(record.agg_avg)
        item["time"] = record.time
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/ground_temperatures/count")
@crossdomain(origin='*')
@authorization
def get_ground_temperatures_count():
    location = request.args.get('location')

    sql_statement = """SELECT locations.name AS loc_name, 
                            observations.height_min_metres AS height, 
                            AVG(observations.numeric_value) as agg_avg, 
                            COUNT(observations.numeric_value) as agg_cnt, 
                            TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM observations.corrected_utc_time) / 86400) * 86400) AT TIME ZONE 'UTC' AS time 
                       FROM observations 
                            INNER JOIN locations ON observations.location = locations.coordinates 
                       WHERE observations.corrected_utc_time BETWEEN '1950-01-01 00:00:00+00' AND '2050-01-01 00:00:00+00' 
                            AND locations.name = :location 
                            AND observations.unit_of_measure = 'C' 
                            GROUP BY observations.height_min_metres, locations.name, time 
                            ORDER BY loc_name ASC, height DESC """
    sql_statement = provider.apply_limit_and_offset(sql_statement)
    sql_statement = provider.apply_count(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'location': location })
    array = []
    for record in records:
        item = {}
        item["count"] = record.count
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/ground_temperatures/height")
@crossdomain(origin='*')
@authorization
def get_ground_temperature_height():
    location = request.args.get('location')

    sql_statement = """SELECT DISTINCT observations.height_min_metres AS height 
                       FROM observations 
                            INNER JOIN locations ON observations.location = locations.coordinates 
                       WHERE observations.corrected_utc_time BETWEEN '1950-01-01 00:00:00+00' AND '2050-01-01 00:00:00+00' 
                            AND locations.name = :location 
                            AND observations.unit_of_measure = 'C' 
                            GROUP BY observations.height_min_metres, locations.name, observations.corrected_utc_time"""
    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'location': location })
    array = []
    for record in records:
        item = {}
        item["height"] = float(record.height)
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/ground_thermal_regime")
@crossdomain(origin='*')
@authorization
def get_ground_thermal_regime():
    location = request.args.get('location')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    sql_statement = """SELECT locations.name AS loc_name, observations.
                                height_min_metres AS height, 
                                MAX(observations.numeric_value) AS max, 
                                MIN(observations.numeric_value) AS min, 
                                AVG(observations.numeric_value) as average_value, 
                                COUNT(observations.numeric_value) as cnt 
                          FROM observations 
                               INNER JOIN locations ON observations.location = locations.coordinates 
                         WHERE observations.corrected_utc_time BETWEEN :start_date 
                           AND :end_date 
                           AND locations.name = :location 
                           AND observations.unit_of_measure = 'C' 
                        GROUP BY observations.height_min_metres, locations.name 
                        ORDER BY loc_name ASC, height DESC; """
    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, {
                                                                        'location': location,
                                                                        'start_date': start_date,
                                                                        'end_date': end_date
                                                                    })
    array = []
    for record in records:
        item = {}
        item["loc_name"] = record.loc_name
        item["height"] = float(record.height)
        item["max"] = float(record.max)
        item["min"] = float(record.min)
        item["average_value"] = float(record.average_value)
        item["cnt"] = float(record.cnt)
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/observations/range")
@crossdomain(origin='*')
@authorization
def get_observations_range():
    location = request.args.get('location')
    category = request.args.get('category')

    sql_statement = """SELECT name, label,
                                height_max_metres as ffrom,
                                height_min_metres as tto,
                                numeric_value,
                                text_value
                           FROM observations
                                JOIN locations
                                  ON ST_Intersects(observations.location, locations.coordinates)
                                INNER JOIN sensors
                                  ON sensors.id = observations.sensor_id
                          WHERE locations.name = :location"""

    if category is not None:
        sql_statement = sql_statement + """ AND sensors.label = :category"""

    sql_statement = sql_statement + """ AND sensors.label IN ('geo_class_1', 'ice_visual_perc',
                                                 'ice_description', 'geo_description')
                       ORDER BY label ASC, ffrom DESC"""

    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'location': location, 'category': category })
    array = []
    for record in records:
        item = {}
        item["name"] = record.name
        item["label"] = record.label
        item["from"] = float(record.ffrom)
        item["to"] = float(record.tto)
        item["numeric_value"] = float(record.numeric_value) if not math.isnan(float(record.numeric_value)) else ""
        item["text_value"] = record.text_value
        array.append(item)
    return jsonify(array)

@permafrost_observations_bp.route("/observations/categories")
@crossdomain(origin='*')
@authorization
def get_observations_categories():
    location = request.args.get('location')

    sql_statement = """SELECT DISTINCT label
                           FROM observations
                                JOIN locations
                                  ON ST_Intersects(observations.location, locations.coordinates)
                                INNER JOIN sensors
                                  ON sensors.id = observations.sensor_id
                          WHERE locations.name = :location
                            AND sensors.label IN ('geo_class_1', 'ice_visual_perc',
                                                 'ice_description', 'geo_description')"""

    sql_statement = provider.apply_limit_and_offset(sql_statement)

    records = provider.execute_sql_and_fetch_records(sql_statement, { 'location': location })
    array = []
    for record in records:
        item = {}
        item["label"] = record.label
        array.append(item)
    return jsonify(array)
