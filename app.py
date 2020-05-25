#
# Entry point to the application
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from permafrost_observations_api.web.views import *
from permafrost_observations_api import permafrost_observations_factory

app = permafrost_observations_factory.create_app(__name__)
app.app_context().push()
permafrost_observations_factory.register_blueprints(app)

if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=7002)

