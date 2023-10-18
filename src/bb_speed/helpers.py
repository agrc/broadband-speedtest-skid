import random

import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
import requests


def _assign_h3(df, resolution):
    df[f'h3_{resolution}'] = df.apply(
        lambda row: h3.h3_to_string(h3.geo_to_h3(row['SHAPE']['y'], row['SHAPE']['x'], resolution)), axis=1
    )


def load_speedtest_data(base_url, params):

    response = requests.get(base_url, params=params, timeout=60)
    cleaned_response = response.text.replace('\n', '')

    data_df = pd.read_xml(cleaned_response)
    spatial_data = pd.DataFrame.spatial.from_xy(data_df, 'longitude', 'latitude')

    for resolution in [5, 6, 7, 8, 9, 12]:
        _assign_h3(spatial_data, resolution)

    return spatial_data


def classify_speedtest_data(speedtest_df):
    conditions = [
        (speedtest_df['dl'] >= 100) & (speedtest_df['ul'] >= 20),  # 1
        ((speedtest_df['dl'] >= 100) & ((speedtest_df['ul'] < 20) & (speedtest_df['ul'] >= 3))) |
        ((speedtest_df['ul'] >= 3) & ((speedtest_df['dl'] < 100) & (speedtest_df['dl'] >= 20))),
        (speedtest_df['dl'] < 25) | (speedtest_df['ul'] < 3),
    ]
    choices = ['above 100/20', 'between 100/20 and 25/3', 'under 25/3']
    speedtest_df['classification'] = np.select(conditions, choices, default='n/a')

    return speedtest_df


def jitter_shape_series(dataframe, group_range, individual_range):
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


def load_census_data(base_url, params):

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


def calc_county_summary(household_df, all_tests_df):
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
