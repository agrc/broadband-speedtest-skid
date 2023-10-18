import pytest
import pandas as pd
import pandas.testing as tm

from bb_speed import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch('pathlib.Path.exists', return_value=True)
    mocker.patch('pathlib.Path.read_text', return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {'foo': 'bar'}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch('pathlib.Path.exists', new=exists_mock)
    mocker.patch('pathlib.Path.read_text', return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {'foo': 'bar'}
    assert exists_mock.call_count == 2


def test_load_census_data(mocker):
    requests_mock = mocker.patch('bb_speed.main.requests')
    requests_mock.get.return_value.json.return_value = [['DP02_0001E', 'state', 'county'], ['87802', '49', '057']]

    output_df = main._load_census_data('', '')

    test_df = pd.DataFrame([['87802', 'Weber County']], columns=['total_households', 'name'])

    tm.assert_frame_equal(test_df, output_df, check_names=False)


def test_calc_county_summary(mocker):

    # households_df = pd.DataFrame([['10', 'Weber County'], ['20', 'Utah County']], columns=['total_households', 'name'])
    households_df = pd.DataFrame({
        'total_households': [10, 20],
        'name': ['Weber County', 'Utah County'],
    })
    speedtests_df = pd.DataFrame({
        'county': ['Weber County', 'Weber County', 'Utah County', 'Utah County'],
        'id': [1, 1, 1, 1]
    })

    summary_df = main._calc_county_summary(households_df, speedtests_df)

    test_df = pd.DataFrame({
        'tests': [2, 2],
        'total_households': [20, 10],
        'name': ['Utah County', 'Weber County'],
        'percent_response': [.1, .2],
    })

    tm.assert_frame_equal(test_df, summary_df, check_dtype=False)
