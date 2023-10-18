#!/usr/bin/env python
# * coding: utf8 *
"""
Run the speedtest script as a cloud function.
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import arcgis
import google.auth
from google.cloud import bigquery
from palletjack import transform, load

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, helpers
except ImportError:
    import config
    import helpers

module_logger = logging.getLogger(config.SKID_NAME)


def _get_secrets():
    """A helper method for loading secrets from either a GCF mount point or the local src/skidname/secrets/secrets.json
    file

    Raises:
        FileNotFoundError: If the secrets file can't be found.

    Returns:
        dict: The secrets .json loaded as a dictionary
    """

    secret_folder = Path('/secrets')

    #: Try to get the secrets from the Cloud Function mount point
    if secret_folder.exists():
        return json.loads(Path('/secrets/app/secrets.json').read_text(encoding='utf-8'))

    #: Otherwise, try to load a local copy for local development
    secret_folder = (Path(__file__).parent / 'secrets')
    if secret_folder.exists():
        return json.loads((secret_folder / 'secrets.json').read_text(encoding='utf-8'))

    raise FileNotFoundError('Secrets folder not found; secrets not loaded.')


def _initialize():
    """A helper method to set up logging
    """

    skid_logger = logging.getLogger(config.SKID_NAME)
    skid_logger.setLevel(config.LOG_LEVEL)
    palletjack_logger = logging.getLogger('palletjack')
    palletjack_logger.setLevel(config.LOG_LEVEL)

    cli_handler = logging.StreamHandler(sys.stdout)
    cli_handler.setLevel(config.LOG_LEVEL)
    formatter = logging.Formatter(
        fmt='%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )
    cli_handler.setFormatter(formatter)

    skid_logger.addHandler(cli_handler)
    palletjack_logger.addHandler(cli_handler)

    #: Log any warnings at logging.WARNING
    #: Put after everything else to prevent creating a duplicate, default formatter
    #: (all log messages were duplicated if put at beginning)
    logging.captureWarnings(True)


def _update_speedtest_points(gis, new_data_df):

    module_logger.info('Classifying and cleaning new data...')
    cleaned_df = helpers.classify_speedtest_data(new_data_df)

    module_logger.info('Jittering new points...')
    module_logger.debug('Projecting data to UTM')
    cleaned_df.spatial.project(26912)
    cleaned_df.spatial.sr = {'wkid': 26912}

    module_logger.debug('Jittering')
    cleaned_df[['new_SHAPE']] = cleaned_df.groupby('h3_12').apply(helpers.jitter_shape_series, (-150, 150), (-20, 20))
    jittered_df = cleaned_df.drop(columns='SHAPE').rename(columns={'new_SHAPE': 'SHAPE'}, copy=True)
    jittered_df.spatial.set_geometry('SHAPE')

    module_logger.debug('Projecting data back to WGS84')
    jittered_df.spatial.project(4326)
    jittered_df.spatial.sr = {'wkid': 4326}

    jittered_df['test_date'] = jittered_df['timestamp']  #: copy the timestamp field for new datetime-aware field
    upload_df = transform.DataCleaning.switch_to_datetime(jittered_df, ['test_date'])
    upload_df = transform.DataCleaning.switch_to_float(upload_df, ['id'])
    upload_df.drop(
        columns=['email', 'ip', 'cost', 'ASN', 'longitude', 'latitude', 'coop', 'tribal', 'h3_12', 'wouldpay'],
        inplace=True,
        errors='ignore'
    )

    type_changes = {'blockid': float, 'mnc': float, 'repeats': float, 'mcc': float}
    upload_df = upload_df.astype(type_changes)

    module_logger.info('Uploading new points...')
    added_features = load.FeatureServiceUpdater.add_features(gis, config.FEATURE_LAYER_ITEMID, upload_df)

    return added_features


def _update_counties(gis, speedtest_df):

    module_logger.info('Updating county summaries...')
    module_logger.debug('Downloading and calculating new county data...')
    census_household_info_df = helpers.load_census_data(config.CENSUS_URL, config.CENSUS_PARAMS)
    all_tests_df = speedtest_df.copy()
    if config.INSTITUTIONS_TO_REMOVE:
        all_tests_df = all_tests_df[~all_tests_df['ispinfo'].isin(config.INSTITUTIONS_TO_REMOVE)].copy()
    county_info_df = helpers.calc_county_summary(census_household_info_df, all_tests_df)

    module_logger.debug('Merging into live county data...')
    live_counties_df = transform.FeatureServiceMerging.get_live_dataframe(gis, config.COUNTIES_ITEMID)
    merged_df = transform.FeatureServiceMerging.update_live_data_with_new_data(live_counties_df, county_info_df, 'name')
    merged_df.drop(columns=['SHAPE'], inplace=True)
    merged_df = transform.DataCleaning.switch_to_float(merged_df, ['tests'])

    updated_counties = load.FeatureServiceUpdater.update_features(
        gis, config.COUNTIES_ITEMID, merged_df, update_geometry=False
    )

    return updated_counties


def process():
    """The main function that does all the work.
    """

    #: Set up logging, secrets
    _initialize()
    secrets = SimpleNamespace(**_get_secrets())

    #: Get our GIS object via the ArcGIS API for Python
    gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER, secrets.AGOL_PASSWORD)

    module_logger.info('Loading new and live speedtest data...')
    live_df = transform.FeatureServiceMerging.get_live_dataframe(gis, config.FEATURE_LAYER_ITEMID)
    speedtest_df = helpers.load_speedtest_data(config.SPEEDTEST_BASE_URL, {'state': 'Utah', 'record': '0'})

    #: Filter out existing and institutional records
    new_data_df = speedtest_df[~speedtest_df['id'].isin(list(live_df['id']))]
    if config.INSTITUTIONS_TO_REMOVE:
        new_data_df = new_data_df[~new_data_df['ispinfo'].isin(config.INSTITUTIONS_TO_REMOVE)].copy()
        new_data_df.reset_index(inplace=True, drop=True)
        new_data_df.spatial.set_geometry('SHAPE')

    added_features = updated_counties = 0  #: init to 0 in case no adds necessary
    if not new_data_df.empty:
        #: Speedtest data
        added_features = _update_speedtest_points(gis, new_data_df)
        module_logger.debug('%s speedtest points added', added_features)

        #: County Summaries
        updated_counties = _update_counties(gis, speedtest_df)
        module_logger.debug('%s county summaries updated', updated_counties)


def main(event, context):  # pylint: disable=unused-argument
    """Entry point for Google Cloud Function triggered by pub/sub event

    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """

    #: This function must be called 'main' to act as the Google Cloud Function entry point. It must accept the two
    #: arguments listed, but doesn't have to do anything with them (I haven't used them in anything yet).

    #: Call process() and any other functions you want to be run as part of the skid here.
    process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == '__main__':
    process()
