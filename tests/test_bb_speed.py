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
    response_mock = mocker.patch.object('main.requests.get')
    response_mock.json().return_value = [['DP02_0001E', 'state', 'county'], ['87802', '49', '057']]

    output_df = main._load_census_data('', '')

    test_df = pd.DataFrame({
        'DP02_0001E': '87802',
        'state': '49',
        'county': '057'
    })

    tm.assert_frame_equal(test_df, output_df)
