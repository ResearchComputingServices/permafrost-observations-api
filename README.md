#
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

Installation and running steps:

Step 1 - Download and install Python 3.7.4 64-bit
    https://www.python.org/downloads/windows/

Step 2 - Install virtualenv
    pip install virtualenv

Step 3 - Create a virtual environment using virtualenv
    cd c:\development\flask-seed
    mkvirtualenv -a . -p "C:\Users\sergiubuhatel\AppData\Local\Programs\Python\Python37\python.exe" env

Step 4 - Activate the virtual environment
    workon
    cd c:\development\flask-seed
    workon env

Step 5 - Install dependencies into the current active virtual environment
    pip install -r requirements.txt

Step 6 - See what dependencies are currently installed
    pip freeze

Step 7 - Launch the server
    python app.py

Step 8 - Run in a browser
    http://127.0.0.1:5002/permafrost_observations_api/locations_of_observations?geometry_type=ST_Point&name_pattern=%RV%
    http://127.0.0.1:5002/permafrost_observations_api/ground_temperatures?location=NGO-RC-165_ST01&limit=10&offset=0
