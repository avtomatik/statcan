#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 16:01:08 2023

@author: green-machine
"""


import pandas as pd
from pandas import DataFrame


def read_can_groupby(file_id: int) -> DataFrame:
    """


    Parameters
    ----------
    file_id : int
        DESCRIPTION.

    Returns
    -------
    DataFrame
        DESCRIPTION.

    """
    FILE_IDS = (5245628780870031920, 7931814471809016759, 8448814858763853126)
    SKIPROWS = (3, 241, 81)
    kwargs = {
        'filepath_or_buffer': f'dataset_can_cansim{file_id:n}.csv',
        'index_col': 0,
        'skiprows': dict(zip(FILE_IDS, SKIPROWS)).get(file_id),
        'parse_dates': file_id == 5245628780870031920
    }

    df = pd.read_csv(**kwargs)
    if file_id == 7931814471809016759:
        df.columns = map(int, map(lambda _: _[:7].split()[-1], df.columns))
        df.iloc[:, -1] = df.iloc[:, -1].str.replace(
            ';', ''
        ).apply(pd.to_numeric)
        df = df.transpose()
    if file_id == 5245628780870031920:
        return df.groupby(df.index.year).mean().rename_axis('period')
    return df.groupby(df.index).mean().rename_axis('period')
