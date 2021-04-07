#
# Raw SQL self.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from sqlalchemy.sql import text
from flask import json, jsonify, Response, blueprints, request
from permafrost_observations_api.extensions import db, ma
from collections import namedtuple

class RawSqlProvider:
    def execute_sql_and_fetch_records(self, sql_statement, params):
        results = self.execute_sql(sql_statement, params)
        return self.fetch_records(results)

    def apply_limit_and_offset(self, sql_statement):
        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit != None:
            sql_statement = sql_statement + ' LIMIT ' + limit
        if offset != None:
            sql_statement = sql_statement + ' OFFSET ' + offset
        return sql_statement

    def apply_count(self, sql_statement):
        return "SELECT COUNT(*) FROM (" + sql_statement + " ) src"

    def execute_sql(self, sql_statement, params):
        results = db.session.execute(text(sql_statement), params)
        db.session.commit()
        return results

    def fetch_records(self, results):
        Record = namedtuple('Record', results.keys())
        records = [Record(*r) for r in results.fetchall()]
        return records

    def float_or_null(self, value):
        return float(value) if value != None else value

    def jsonify_records(self, records):
        array = []
        for record in records:
            item = {}
            item["name"] = record.name
            item["text"] = record.name
            item["lat"] = record.lat
            item["lon"] = record.lon
            item["lng"] = record.lon
            item["elevation_in_metres"] = self.float_or_null(record.elevation_in_metres)
            item["comment"] = record.comment
            item["record_observations"] = record.record_observations
            item["accuracy_in_metres"] = self.float_or_null(record.accuracy_in_metres)
            item["provider"] = record.provider
            array.append(item)
        return jsonify(array)

    def jsonify_count_records(self, records):
        array = []
        for record in records:
            item = {}
            item["count"] = record.count
            array.append(item)
        return jsonify(array)

    def write_observation_time_temperature_into_file(self, fullpath, location):
        with open(fullpath, 'w') as f:
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
            sql_statement = self.apply_limit_and_offset(sql_statement)

            records = self.execute_sql_and_fetch_records(sql_statement, {'location': location})
            f.write("name,height,agg_avg,time\n")
            for record in records:
                f.write(record.loc_name + ',' + str(record.height) + ',' + str(record.agg_avg) + ',' + str(record.time) + "\n")

    def write_observation_temperature_height_into_file(self, fullpath, location, start_date, end_date):
        with open(fullpath, 'w') as f:
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
            sql_statement = self.apply_limit_and_offset(sql_statement)

            records = self.execute_sql_and_fetch_records(sql_statement, {
                                                                            'location': location,
                                                                            'start_date': start_date,
                                                                            'end_date': end_date
                                                                        })
            f.write("name,height,max,min,average_value,cnt\n")
            for record in records:
                f.write(record.loc_name + ',' + str(record.height) + ',' + str(record.max) + ',' + str(record.min) +
                        ',' + str(record.average_value) + ',' + str(record.cnt) + "\n")
