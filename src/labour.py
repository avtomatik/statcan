# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import os
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from statcan.src.core.funcs import combine_can_special

from ....statcan.src.core.constants import (SERIES_IDS_INDEXES,
                                            SERIES_IDS_PERSONS,
                                            SERIES_IDS_THOUSANDS)
from ..common.funcs import dichotomize_series_ids
from .get_mean_for_min_std import get_mean_for_min_std


def get_data_frame_index(series_ids: tuple[dict[str, int]]) -> DataFrame:
    df = pd.concat(
        map(
            lambda _: combine_can_special(
                *dichotomize_series_ids(_, TO_PARSE_DATES)
            ),
            series_ids
        ),
        axis=1,
        sort=True
    ).pct_change()
    df['mean'] = df.mean(axis=1)
    # =========================================================================
    # Composite Index
    # =========================================================================
    df['composite'] = df.iloc[:, [-1]].add(1).cumprod()
    # =========================================================================
    # Patch First Element
    # =========================================================================
    df['composite'] = df['composite'].fillna(1)
    return df.iloc[:, [-1]]


def get_data_frame_value(
    series_ids_thousands: tuple[dict[str, int]],
    series_ids_persons: tuple[dict[str, int]]
) -> DataFrame:
    df = pd.concat(
        [
            pd.concat(
                map(
                    lambda _: combine_can_special(
                        *dichotomize_series_ids(_, TO_PARSE_DATES)
                    ),
                    series_ids_thousands
                ),
                axis=1
            ),
            pd.concat(
                map(
                    lambda _: combine_can_special(
                        *dichotomize_series_ids(_, TO_PARSE_DATES)
                    ).div(1000),
                    series_ids_persons
                ),
                axis=1
            )
        ],
        axis=1,
        sort=True
    )
    df['mean'] = df.mean(axis=1)
    return df.iloc[:, [-1]]


PATH_SOURCE = '../../../data/external'
PATH_EXPORT = '/home/green-machine/Downloads'

TO_PARSE_DATES = (
    2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
)

# =============================================================================
# Labor
# =============================================================================
SERIES_IDS = {
    '!v41707595': 36100309,  # Not Useful: Labour input
    '!v41712954': 36100208,  # Not Useful: Labour input
    '!v42189231': 36100310,  # Not Useful: Labour input
    '!v65522120': 36100489,  # Not Useful
    '!v65522415': 36100489,  # Not Useful
}


os.chdir(PATH_SOURCE)


def main(path_export, SERIES_IDS_INDEXES, SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS):
    df_index = get_data_frame_index(SERIES_IDS_INDEXES)

    # =============================================================================
    # df_value = get_data_frame_value(SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS)
    # =============================================================================

    df = DataFrame()

    year, value = get_mean_for_min_std(SERIES_IDS_THOUSANDS, TO_PARSE_DATES)

    df['workers'] = df_index.div(df_index.loc[year, :]).mul(value).round(1)

    FILE_NAME = 'can_labour.pdf'
    kwargs = {
        'fname': Path(path_export).joinpath(FILE_NAME),
        'format': 'pdf',
        'dpi': 900
    }
    df.plot(grid=True).get_figure().savefig(**kwargs)


if __name__ == '__main__':
    main(
        PATH_EXPORT,
        SERIES_IDS_INDEXES,
        SERIES_IDS_THOUSANDS,
        SERIES_IDS_PERSONS
    )
