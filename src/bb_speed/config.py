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

FEATURE_LAYER_ITEMID = ''
JOIN_COLUMN = ''
ATTACHMENT_LINK_COLUMN = ''
ATTACHMENT_PATH_COLUMN = ''
FIELDS = {

}
