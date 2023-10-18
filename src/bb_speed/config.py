"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket

SKID_NAME = 'speedtest'

AGOL_ORG = 'https://utah.maps.arcgis.com'
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    'from_address': 'noreply@utah.gov',
    'to_addresses': 'jdadams@utah.gov',
    'prefix': f'{SKID_NAME} on {socket.gethostname()}: ',
}
LOG_LEVEL = logging.DEBUG
LOG_FILE_NAME = 'log'

SPEEDTEST_BASE_URL = 'https://expressoptimizer.net/APIgetstate.php'
# SPEEDTEST_URL_PARAMS = {'state': 'Utah', 'record': '0'}

CENSUS_URL = 'https://api.census.gov/data/2021/acs/acs5/profile'
CENSUS_PARAMS = {'get': 'DP02_0001E', 'for': 'county:*', 'in': 'state:49'}

#: If this list is populated, remove any points that have these values in their isp column
INSTITUTIONS_TO_REMOVE = [
    'State of Utah',
    'Utah Education Network',
    'Utah State University',
    'University of Utah',
    'Salt Lake City Corporation',
    'Brigham Young University',
    'Salt Lake Community College',
]

FEATURE_LAYER_ITEMID = 'abf5150619294eb182f6cc0a669ff0ff'
COUNTIES_ITEMID = '07cdf8a74e7e4c4e93c854f281cdff5f'
JOIN_COLUMN = ''
ATTACHMENT_LINK_COLUMN = ''
ATTACHMENT_PATH_COLUMN = ''
FIELDS = {}
