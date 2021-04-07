#
# Handle CORS (cross origin)
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from datetime import timedelta
from functools import update_wrapper

from flask import request, current_app, make_response

def crossdomain(origin=None, methods=None, headers=None,
                exposed_headers=None, max_age=21600,
                attach_to_all=True, automatic_options=True):
    """Decorator function that allows crossdomain requests.
      Courtesy of
      https://blog.skyred.fi/articles/better-crossdomain-snippet-for-flask.html

      Usage Example:

      .. code-block:: python

          @BOBI.route('/servers', methods=['GET'])
          @crossdomain(origin='*')
          def list_servers():
          :param max_age:
    """
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, str):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, str):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        """ Determines which methods are allowed
        """
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        """The decorator function
        :param f:
        """

        def wrapped_function(*args, **kwargs):
            """Caries out the actual cross domain code
            """
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers
            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            h['Access-Control-Allow-Credentials'] = 'true'
            h['Access-Control-Allow-Headers'] = \
                "Origin, X-Requested-With, Content-Type, Accept, Authorization"
            h['Access-Control-Expose-Headers'] = \
                'Location, X-Epoch, X-Safety-Off, ' \
                'X-Total-Items, X-Set, X-Items-Per-Set'
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            if exposed_headers is not None:
                h['Access-Control-Expose-Headers'] = exposed_headers
            return resp

        f.provide_automatic_options = False
        f.required_methods = ['OPTIONS']
        return update_wrapper(wrapped_function, f)

    return decorator
