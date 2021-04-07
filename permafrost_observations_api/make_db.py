#
# Create database schema based on the model specification.
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import sys
sys.path.append('../')

from permafrost_observations_api.extensions import db, ma
from permafrost_observations_api import permafrost_observations_factory

app = permafrost_observations_factory.create_app(__name__)

with app.app_context():
    db.engine.execute("drop schema if exists public cascade")
    db.engine.execute("create schema public")

    db.reflect()
    db.drop_all()
    db.create_all()

    db.session.commit()

