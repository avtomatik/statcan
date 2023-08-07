#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 20:44:49 2023

@author: green-machine
"""


import io
from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from pandas import DataFrame

from statcan.src.core.constants import MAP_READ_CAN, MAP_READ_CAN_SPC
from thesis.src.core.pull import (pull_by_series_id, pull_can_capital,
                                  pull_can_capital_former)
from thesis.src.core.transform import (transform_stockpile, transform_sum,
                                       transform_year_sum)


@cache
def read_can(archive_id: int) -> DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 10, 12]))
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    kwargs = {
        'header': 0,
        'names': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }

    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
                requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
            pass
    return pd.read_csv(**kwargs)


@cache
def read_can_sandbox(archive_id: int) -> DataFrame:
    """
    Parameters
    ----------
    archive_id : int
    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 10, 12]))
    MAP_ARCHIVE_ID_FIELD = {
        310004: dict(zip(['period', 'prices', 'category', 'component', 'series_id', 'value'], [0, 2, 4, 5, 6, 8])),
        2820011: dict(zip(['period', 'geo', 'classofworker', 'industry', 'sex', 'series_id', 'value'], [0, 1, 2, 3, 4, 5, 7])),
        2820012: dict(zip(['period', 'series_id', 'value'], [0, 5, 7])),
        3790031: dict(zip(['period', 'geo', 'seas', 'prices', 'naics', 'series_id', 'value'], [0, 1, 2, 3, 4, 5, 7])),
        3800084: dict(zip(['period', 'geo', 'seas', 'est', 'series_id', 'value'], [0, 1, 2, 3, 4, 6])),
        3800102: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        3800106: dict(zip(['period', 'series_id', 'value'], [0, 3, 5])),
        3800518: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        3800566: dict(zip(['period', 'series_id', 'value'], [0, 3, 5])),
        3800567: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        36100096: dict(
            zip(
                [
                    'period',
                    # =============================================================================
                    #                 'geo', 'prices', 'industry', 'category', 'component',
                    # =============================================================================
                    'series_id', 'value'
                ],
                [
                    0,
                    # =============================================================================
                    #                 1, 3, 4, 5, 6,
                    # =============================================================================
                    11, 13
                ]
            )
        ),
        36100303: dict(zip(['period', 'series_id', 'value'], [0, 9, 11])),
        36100305: dict(zip(['period', 'series_id', 'value'], [0, 9, 11])),
        36100236: dict(zip(['period', 'series_id', 'value'], [0, 11, 13]))
    }
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    kwargs = {
        'header': 0,
        'names': list(MAP_ARCHIVE_ID_FIELD.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_ARCHIVE_ID_FIELD.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }

    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
                requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


@cache
def read_can(archive_id: int) -> DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 10, 12]))
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    kwargs = {
        'header': 0,
        'names': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }

    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'dataset_can_{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


@cache
def read_can(archive_id: int) -> DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 9, 11]))
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'

    kwargs = {
        'header': 0,
        'names': list(MAP_READ_CAN_SPC.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_READ_CAN_SPC.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }

    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'dataset_can_{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


def combine_can(blueprint: dict) -> DataFrame:
    """
    Parameters
    ----------
    blueprint : dict
        DESCRIPTION.
    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Capital
        df.iloc[:, 1]      Labor
        df.iloc[:, 2]      Product
        ================== =================================
    """
    PATH_SRC = '/media/green-machine/KINGSTON'
    kwargs = {
        'filepath_or_buffer': Path(PATH_SRC).joinpath(f'{tuple(blueprint)[0]}_preloaded.csv'),
    }
    if Path(PATH_SRC).joinpath(f'{tuple(blueprint)[0]}_preloaded.csv').is_file():
        kwargs['index_col'] = 0
        _df = pd.read_csv(**kwargs)
    else:
        function = (
            # =================================================================
            # WARNING : pull_can_capital() : VERY EXPENSIVE OPERATION !
            # =================================================================
            pull_can_capital,
            pull_can_capital_former
        )[max(blueprint) < 10 ** 7]
        _df = read_can(tuple(blueprint)[0]).pipe(
            function, blueprint.get(tuple(blueprint)[0])
        )
        # =====================================================================
        # Kludge
        # =====================================================================
        _df = _df.set_index(_df.iloc[:, 0])
    df = pd.concat(
        [
            # =================================================================
            # Capital
            # =================================================================
            _df.loc[:, ('series_id', 'value')].pipe(
                transform_stockpile
            ).pipe(
                transform_sum, name="capital"
            ),
            # =================================================================
            # Labor
            # =================================================================
            read_can(tuple(blueprint)[1]).pipe(
                pull_by_series_id, blueprint.get(tuple(blueprint)[1])
            ).apply(pd.to_numeric, errors='coerce'),
            # =================================================================
            # Manufacturing
            # =================================================================
            read_can(tuple(blueprint)[-1]).pipe(
                transform_year_sum,
                blueprint.get(tuple(blueprint)[-1])
            ),
        ],
        axis=1
    ).dropna(axis=0)
    df.columns = ('capital', 'labor', 'product')
    return df.div(df.iloc[0, :])


def get_blueprint(year_base: int = 2012) -> dict:
    return {
        # =====================================================================
        # Capital
        # =====================================================================
        36100096: (
            year_base,
            "Manufacturing",
            "Linear end-year net stock",
            (
                "Non-residential buildings",
                "Engineering construction",
                "Machinery and equipment"
            )
        ),
        # =====================================================================
        # Labor : {'v2523012': 14100027}, Preferred Over {'v3437501': 2820011} Which Is Quarterly
        # =====================================================================
        'v2523012': 14100027,
        # =====================================================================
        # Manufacturing
        # =====================================================================
        'v65201809': 36100434,
    }


def get_blueprint_former(year_base: int = 2007) -> dict:
    return {
        # =====================================================================
        # Capital
        # =====================================================================
        310004: (year_base, "Geometric (infinite) end-year net stock", "industrial"),
        # =====================================================================
        # Labor : {'v2523012': 2820012}, Preferred Over {'v3437501': 2820011} Which Is Quarterly
        # =====================================================================
        'v2523012': 2820012,
        # =====================================================================
        # Manufacturing
        # =====================================================================
        'v65201809': 3790031,
    }
