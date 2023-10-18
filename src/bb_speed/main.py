#!/usr/bin/env python
# * coding: utf8 *
"""
Run the speedtest script as a cloud function.
"""
import json
import logging
import random
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
import requests
from palletjack import transform, load
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, version
except ImportError:
    import config
    import version


def _get_secrets():
    """A helper method for loading secrets from either a GCF mount point or the local src/skidname/secrets/secrets.json file

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


def _initialize(log_path, sendgrid_api_key):
    """A helper method to set up logging and supervisor

    Args:
        log_path (Path): File path for the logfile to be written
        sendgrid_api_key (str): The API key for sendgrid for this particular application

    Returns:
        Supervisor: The supervisor object used for sending messages
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

    log_handler = logging.FileHandler(log_path, mode='w')
    log_handler.setLevel(config.LOG_LEVEL)
    log_handler.setFormatter(formatter)

    skid_logger.addHandler(cli_handler)
    skid_logger.addHandler(log_handler)
    palletjack_logger.addHandler(cli_handler)
    palletjack_logger.addHandler(log_handler)

    #: Log any warnings at logging.WARNING
    #: Put after everything else to prevent creating a duplicate, default formatter
    #: (all log messages were duplicated if put at beginning)
    logging.captureWarnings(True)

    skid_logger.debug('Creating Supervisor object')
    skid_supervisor = Supervisor(handle_errors=False)
    sendgrid_settings = config.SENDGRID_SETTINGS
    sendgrid_settings['api_key'] = sendgrid_api_key
    skid_supervisor.add_message_handler(
        SendGridHandler(
            sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
        )
    )

    return skid_supervisor


def _remove_log_file_handlers(log_name, loggers):
    """A helper function to remove the file handlers so the tempdir will close correctly

    Args:
        log_name (str): The logfiles filename
        loggers (List<str>): The loggers that are writing to log_name
    """

    for logger in loggers:
        for handler in logger.handlers:
            try:
                if log_name in handler.stream.name:
                    logger.removeHandler(handler)
                    handler.close()
            except Exception as error:
                pass


def process():
    """The main function that does all the work.
    """

    #: Set up secrets, tempdir, supervisor, and logging
    start = datetime.now()

    secrets = SimpleNamespace(**_get_secrets())

    with TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        log_name = f'{config.LOG_FILE_NAME}_{start.strftime("%Y%m%d-%H%M%S")}.txt'
        log_path = tempdir_path / log_name

        skid_supervisor = _initialize(log_path, secrets.SENDGRID_API_KEY)
        module_logger = logging.getLogger(config.SKID_NAME)

        #: Get our GIS object via the ArcGIS API for Python
        # gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER, secrets.AGOL_PASSWORD)
        gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER)

        #: Speedtest data
        module_logger.info('Loading new and live speedtest data...')
        live_df = transform.FeatureServiceMerging.get_live_dataframe(gis, config.FEATURE_LAYER_ITEMID)
        speedtest_df = _load_speedtest_data(config.SPEEDTEST_BASE_URL, {'state': 'Utah', 'record': '0'})
        new_data_df = speedtest_df[~speedtest_df['id'].isin(list(live_df['id']))]

        module_logger.info('Classifying and cleaning new data...')
        cleaned_df = _classify_speedtest_data(new_data_df)

        if config.INSTITUTIONS_TO_REMOVE:
            cleaned_df = cleaned_df[~cleaned_df['ispinfo'].isin(config.INSTITUTIONS_TO_REMOVE)].copy()
            cleaned_df.reset_index(inplace=True, drop=True)
            cleaned_df.spatial.set_geometry('SHAPE')

        module_logger.info('Jittering new points...')
        module_logger.debug('Projecting data to UTM')
        cleaned_df.spatial.project(26912)
        cleaned_df.spatial.sr = {'wkid': 26912}

        module_logger.debug('Jittering')
        cleaned_df[['new_SHAPE']] = cleaned_df.groupby('h3_12').apply(_jitter_df, (-150, 150), (-20, 20))
        jittered_df = cleaned_df.drop(columns='SHAPE').rename(columns={'new_SHAPE': 'SHAPE'}, copy=True)
        jittered_df.spatial.set_geometry('SHAPE')

        module_logger.debug('Projecting data back to WGS84')
        jittered_df.spatial.project(4326)
        jittered_df.spatial.sr = {'wkid': 4326}

        upload_df = transform.DataCleaning.switch_to_float(jittered_df, ['id'])
        upload_df.drop(
            columns=['email', 'ip', 'cost', 'ASN', 'longitude', 'latitude', 'coop', 'tribal', 'h3_12', 'wouldpay'],
            inplace=True,
            errors='ignore'
        )

        type_changes = {'blockid': float, 'mnc': float, 'repeats': float, 'mcc': float}
        upload_df = upload_df.astype(type_changes)

        module_logger.info('Uploading new points...')
        added_features = load.FeatureServiceUpdater.add_features(gis, config.FEATURE_LAYER_ITEMID, upload_df)

        #: County Summaries
        module_logger.info('Updating county summaries...')
        module_logger.debug('Downloading and calculating new county data...')
        census_household_info_df = _load_census_data(config.CENSUS_URL, config.CENSUS_PARAMS)
        all_tests_df = speedtest_df.copy()
        if config.INSTITUTIONS_TO_REMOVE:
            all_tests_df = all_tests_df[~all_tests_df['ispinfo'].isin(config.INSTITUTIONS_TO_REMOVE)].copy()
        county_info_df = _calc_county_summary(census_household_info_df, all_tests_df)

        module_logger.debug('Merging into live county data...')
        live_counties_df = transform.FeatureServiceMerging.get_live_dataframe(gis, config.COUNTIES_ITMEID)
        merged_df = transform.FeatureServiceMerging.update_live_data_with_new_data(
            live_counties_df, county_info_df, 'name'
        )
        merged_df.drop(columns=['SHAPE'], inplace=True)
        merged_df = transform.DataCleaning.switch_to_float(merged_df, ['tests'])

        updated_counties = load.FeatureServiceUpdater.update_features(
            gis, config.COUNTIES_ITMEID, merged_df, update_geometry=False
        )

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f'{config.SKID_NAME} Update Summary'
        summary_rows = [
            f'{config.SKID_NAME} update {start.strftime("%Y-%m-%d")}',
            '=' * 20,
            '',
            f'Start time: {start.strftime("%H:%M:%S")}',
            f'End time: {end.strftime("%H:%M:%S")}',
            f'Duration: {str(end-start)}',
            #: Add other rows here containing summary info captured/calculated during the working portion of the skid,
            #: like the number of rows updated or the number of successful attachment overwrites.
            f'{added_features} new points added',
            f'{updated_counties} counties\' summaries updated'
        ]

        summary_message.message = '\n'.join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger('palletjack')]
        _remove_log_file_handlers(log_name, loggers)


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


def assign_h3(df, resolution):
    df[f'h3_{resolution}'] = df.apply(
        lambda row: h3.h3_to_string(h3.geo_to_h3(row['SHAPE']['y'], row['SHAPE']['x'], resolution)), axis=1
    )


def _load_speedtest_data(base_url, params):

    response = requests.get(base_url, params=params, timeout=60)
    cleaned_response = response.text.replace('\n', '')

    data_df = pd.read_xml(cleaned_response)
    spatial_data = pd.DataFrame.spatial.from_xy(data_df, 'longitude', 'latitude')

    for resolution in [5, 6, 7, 8, 9, 12]:
        assign_h3(spatial_data, resolution)

    return spatial_data


def _classify_speedtest_data(speedtest_df):
    conditions = [
        (speedtest_df['dl'] >= 100) & (speedtest_df['ul'] >= 20),  # 1
        ((speedtest_df['dl'] >= 100) & ((speedtest_df['ul'] < 20) & (speedtest_df['ul'] >= 3))) |
        ((speedtest_df['ul'] >= 3) & ((speedtest_df['dl'] < 100) & (speedtest_df['dl'] >= 20))),
        (speedtest_df['dl'] < 25) | (speedtest_df['ul'] < 3),
    ]
    choices = ['above 100/20', 'between 100/20 and 25/3', 'under 25/3']
    speedtest_df['classification'] = np.select(conditions, choices, default='n/a')

    return speedtest_df


def _jitter_df(dataframe, group_range, individual_range):
    """Shifts a collection of points by a common random distance and then each point by an individual random distance

    Should be .apply()'d to a groupby on a spatially-enabled dataframe. The grouping should gather points that are
    extremely close together/identical into groups (like points that share an h3 key at level 12). The two-step group
    then individual shift reduces the ability to identify the precise center of a bunch of common points (like multiple
    records at a single home or apartment building) by randomizing the origin point that the individual points'
    jittering starts from.

    The ranges must make sense for the projection your data are in. Recommend projecting lat/long data into something
    that uses feet/meters for ease of explaining the jitter distances.

    Args:
        dataframe (pd.DataFrame): Collection of points to be shifted; usually the individual groupings from a .groupby
            operation
        group_range (Tuple[int]): Min and max values to create new independent x and y shifts for the whole group
        individual_range (Tuple[int]): Min and max values to create new independent x and y shifts for each point.

    Returns:
        pd.DataFrame: A single-column dataframe containing the new, shifted geometries.
    """

    group_jitter = tuple(map(lambda x: random.randint(*group_range), range(2)))
    #: The apply below returns a series, which is then recast to a dataframe so that the return is easily concatted
    new_shape = pd.DataFrame(dataframe.apply(_jitter_row, args=(group_jitter, individual_range), axis=1))
    return new_shape


def _jitter_row(series, group_jitter, individual_range):
    #: returns a scalar (a new shape dict)
    group_x, group_y = group_jitter
    individual_x, individual_y = map(lambda x: random.randint(*individual_range), range(2))
    #: without .copy(), shape is just a reference to the original dict and thus we're mutating the original data
    shape = series['SHAPE'].copy()
    shape['x'] = shape['x'] + group_x + individual_x
    shape['y'] = shape['y'] + group_y + individual_y
    return shape


def _load_census_data(base_url, params):

    response = requests.get(base_url, params=params, timeout=60)
    dataframe = pd.DataFrame(response.json())

    names_to_fips = {
        'BEAVER': '49001',
        'BOX ELDER': '49003',
        'CACHE': '49005',
        'CARBON': '49007',
        'DAGGETT': '49009',
        'DAVIS': '49011',
        'DUCHESNE': '49013',
        'EMERY': '49015',
        'GARFIELD': '49017',
        'GRAND': '49019',
        'IRON': '49021',
        'JUAB': '49023',
        'KANE': '49025',
        'MILLARD': '49027',
        'MORGAN': '49029',
        'PIUTE': '49031',
        'RICH': '49033',
        'SALT LAKE': '49035',
        'SAN JUAN': '49037',
        'SANPETE': '49039',
        'SEVIER': '49041',
        'SUMMIT': '49043',
        'TOOELE': '49045',
        'UINTAH': '49047',
        'UTAH': '49049',
        'WASATCH': '49051',
        'WASHINGTON': '49053',
        'WAYNE': '49055',
        'WEBER': '49057'
    }
    # yapf: disable
    fips_df = (pd.DataFrame.from_dict(names_to_fips, 'index', columns=['fips'])
                .reset_index()
                .rename(columns={'index': 'name'}))
    # yapf: enable

    #: First row is the column names
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].copy().reset_index(drop=True)
    dataframe['cofips'] = dataframe['state'] + dataframe['county']

    dataframe = dataframe.merge(fips_df, left_on='cofips', right_on='fips')
    dataframe['name'] = dataframe['name'].str.title() + ' County'
    dataframe.drop(columns=['state', 'county', 'cofips', 'fips'], inplace=True)
    dataframe.rename(columns={'DP02_0001E': 'total_households'}, inplace=True)

    return dataframe


def _calc_county_summary(household_df, all_tests_df):
    # yapf: disable
    county_summary = (pd.DataFrame(all_tests_df.groupby('county')['id'].count())
                        .reset_index()
                        .merge(household_df, left_on='county', right_on='name')
                        .drop(columns=['county'])
                        .rename(columns={'id': 'tests'}))
    # yapf: enable
    county_summary['total_households'] = county_summary['total_households'].astype(int)
    county_summary['percent_response'] = county_summary['tests'] / county_summary['total_households']

    return county_summary


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == '__main__':
    process()
