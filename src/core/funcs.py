#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 20:44:49 2023

@author: green-machine
"""


import io
import re
import sqlite3
import zipfile
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from core.config import DATA_DIR
from stats.src.common.transform import transform_year_mean

from statcan.src.core.constants import MAP_READ_CAN, MAP_READ_CAN_SPC
from statcan.src.foreign.funcs import pull_by_series_id


@cache
def read_can(archive_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    pd.DataFrame
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(io.BytesIO(
                requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
            pass
    return pd.read_csv(**kwargs)


@cache
def read_can_sandbox(archive_id: int) -> pd.DataFrame:
    """
    Parameters
    ----------
    archive_id : int
    Returns
    -------
    pd.DataFrame
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(io.BytesIO(
                requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


@cache
def read_can(archive_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    pd.DataFrame
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


@cache
def read_can(archive_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    pd.DataFrame
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


def pull_can_capital(df: pd.DataFrame) -> list[str]:
    """
    Retrieves Series IDs from Statistics Canada -- Fixed Assets Tables
    """
    {
        "table": "031-0004",
        "title": "Flows and stocks of fixed non-residential capital, total all industries, by asset, provinces and territories, annual (dollars x 1,000,000)",
        "file_name": "dataset_can_00310004-eng.zip"
    }
    _filter = (
        (df.iloc[:, 2].str.contains('2007 constant prices')) &
        (df.iloc[:, 4] == 'Geometric (infinite) end-year net stock') &
        (df.iloc[:, 5].str.contains('Industrial', flags=re.IGNORECASE))
    )
    {
        "table": "36-10-0238-01 (formerly CANSIM 031-0004)",
        "title": "Flows and stocks of fixed non-residential capital, total all industries, by asset, provinces and territories, annual (dollars x 1,000,000)"
    }
    _filter = (
        (df.iloc[:, 3].str.contains('2007 constant prices')) &
        (df.iloc[:, 5] == 'Straight-line end-year net stock') &
        (df.iloc[:, 6].str.contains('Industrial', flags=re.IGNORECASE))
    )
    return sorted(set(df[_filter].loc[:, "VECTOR"]))


def pull_can_capital(df: pd.DataFrame, params: tuple[int, str]) -> pd.DataFrame:
    """
    WARNING: VERY EXPENSIVE OPERATION !
    Retrieves Series IDs from Statistics Canada -- Fixed Assets Tables

    Parameters
    ----------
    df : pd.DataFrame

    params : tuple[int, str]
        param : YEAR_BASE : Basic Price Year.
        param : CATEGORY : Estimate Basis.
        param : COMPONENT : Search Key Word

    Returns
    -------
    pd.DataFrame

    """
    DBNAME = "capital"
    stmt = f"""
    SELECT * FROM {DBNAME}
    WHERE
        geo = 'Canada'
        AND prices LIKE '%{params[0]} constant prices%'
        AND industry = '{params[1]}'
        AND category = '{params[2]}'
        AND component IN {params[-1]}
    ;
    """
    db_path = DATA_DIR.joinpath(f"{DBNAME}.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        df.to_sql(DBNAME, conn, if_exists="replace", index=True)
        cursor = conn.execute(stmt)
        return pd.DataFrame(
            cursor.fetchall(),
            columns=("period", "geo", "prices", "industry", "category",
                     "component", "series_id", 'value')
        )


def pull_can_capital_former(df: pd.DataFrame, params: tuple[int, str]) -> pd.DataFrame:
    """
    Retrieves Series IDs from Statistics Canada -- Fixed Assets Tables

    Parameters
    ----------
    df : pd.DataFrame

    params : tuple[int, str]
        param : YEAR_BASE : Basic Price Year.
        param : CATEGORY : Estimate Basis.
        param : COMPONENT : Search Key Word

    Returns
    -------
    pd.DataFrame

    """
    DBNAME = "capital"
    stmt = f"""
    SELECT * FROM {DBNAME}
    WHERE
        prices LIKE '%{params[0]} constant prices%'
        AND category = '{params[1]}'
        AND lower(component) LIKE '%{params[-1]}%'
    ;
    """
    db_path = DATA_DIR.joinpath(f"{DBNAME}.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        df.to_sql(DBNAME, conn, if_exists="replace", index=True)
        cursor = conn.execute(stmt)
        return pd.DataFrame(
            cursor.fetchall(),
            columns=("period", "prices", "category", "component",
                     "series_id", 'value')
        )


def transform_stockpile(df: pd.DataFrame) -> pd.DataFrame:
    """


    Parameters
    ----------
    df : pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series IDs
        df.iloc[:, 1]      Values
        ================== =================================
    name : str
        New Column Name.

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Sum of <series_ids>
        ================== =================================
    """
    return pd.concat(
        map(
            lambda _: df.pipe(pull_by_series_id, _),
            sorted(set(df.iloc[:, 0]))
        ),
        axis=1
    ).apply(pd.to_numeric, errors='coerce')


def transform_sum(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """


    Parameters
    ----------
    df : pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, ...]    Series
        ================== =================================
    name : str
        New Column Name.

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Sum of <series_ids>
        ================== =================================
    """
    df[name] = df.sum(axis=1)
    return df.iloc[:, [-1]]


def transform_year_sum(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(df.index.year).sum()


def combine_can(blueprint: dict) -> pd.DataFrame:
    """
    Parameters
    ----------
    blueprint : dict
        DESCRIPTION.
    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Capital
        df.iloc[:, 1]      Labor
        df.iloc[:, 2]      Product
        ================== =================================
    """
    FILE_NAME = f'{tuple(blueprint)[0]}_preloaded.csv'
    kwargs = {
        'filepath_or_buffer': DATA_DIR.joinpath(FILE_NAME),
    }
    if DATA_DIR.joinpath(f'{tuple(blueprint)[0]}_preloaded.csv').is_file():
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


def archive_name_to_url(archive_name: str) -> str:
    """
    Parameters
    ----------
    archive_name : str
        DESCRIPTION.
    Returns
    -------
    str
        DESCRIPTION.
    """
    return f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_name}'


def stockpile_can(series_ids: dict[str, int]) -> pd.DataFrame:
    """
    Parameters
    ----------
    series_ids : dict[str, int]
        DESCRIPTION.
    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    return pd.concat(
        map(
            lambda _: read_can(_[-1]).pipe(pull_by_series_id, _[0]),
            series_ids.items()
        ),
        axis=1,
        sort=True
    )


def combine_can_special(
    series_ids_plain: dict[str, int],
    series_ids_mean: dict[str, int]
) -> pd.DataFrame:
    if series_ids_plain:
        return combine_can_plain_or_sum(series_ids_plain)
    if series_ids_mean:
        return combine_can_plain_or_sum(series_ids_mean).pipe(transform_year_mean)


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    FILTER = (
        (df.loc[:, 'naics'] == 'All industries (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201756')
    )
    FILTER = (
        (df.loc[:, 'naics'] == 'Manufacturing (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201809')
    )
    return df[FILTER].iloc[:, -2:]


def get_kwargs_can() -> dict[str, Any]:

    ARCHIVE_ID = 3790031
    NAMES = ['period', 'geo', 'seas', 'prices', 'naics', 'series_id', 'value']
    USECOLS = [0, 1, 2, 3, 4, 5, 7]

    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )

    FILE_NAME = f'dataset_can_{ARCHIVE_ID:08n}-eng.zip'
    return {
        'filepath_or_buffer': DATA_DIR.joinpath(FILE_NAME),
        'header': 0,
        'names': NAMES,
        'index_col': 0,
        'usecols': USECOLS,
        'parse_dates': ARCHIVE_ID in TO_PARSE_DATES
    }
