import pandas as pd

from bb_speed import helpers


def test_assign_speed_categories():
    test_df = pd.DataFrame({
        'dl': [150, 150, 150, 50, 50, 50, 1, 1, 1],
        'ul': [25, 5, 1, 25, 5, 1, 25, 5, 1],
    })

    helpers.assign_speed_categories(test_df)

    expected = [
        'above 100/20',
        'between 100/20 and 25/3',
        'under 25/3',
        'between 100/20 and 25/3',
        'between 100/20 and 25/3',
        'under 25/3',
        'under 25/3',
        'under 25/3',
        'under 25/3',
    ]

    assert test_df['classification'].to_list() == expected


def test_jitter_xy():
    rows = [{'SHAPE': {'x': 1, 'y': 1}}] * 100
    new_rows = []
    for row in rows:
        new_rows.append(helpers.jitter_xy(row))

    for row in new_rows:
        x, y = row
        assert -140 < x < -60 or 60 < x < 140
        assert -140 < y < -60 or 60 < y < 140
