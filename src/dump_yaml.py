#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 11:09:07 2023

@author: green-machine
"""


from pathlib import Path

import yaml

from statcan.src.core.constants import (DATA_CONSTRUCT_CAN_FORMER,
                                        DATA_CONSTRUCT_CAN_FORMER_NOT_USED,
                                        DATA_STATCAN_ARCHIVE)


def dump(path_exp: str, file_name: str, data: dict) -> None:
    with open(Path(path_exp).joinpath(file_name), 'w') as f:
        yaml.dump(data, f, default_flow_style=False)


def main(path_exp: str = '/home/green-machine/Downloads') -> None:

    file_name = 'combine_can_former.yaml'
    dump(path_exp, file_name, DATA_CONSTRUCT_CAN_FORMER)

    file_name = 'combine_can_former_not_used.yaml'
    dump(path_exp, file_name, DATA_CONSTRUCT_CAN_FORMER_NOT_USED)

    file_name = 'statcan_archive.yaml'
    dump(path_exp, file_name, DATA_STATCAN_ARCHIVE)


if __name__ == '__main__':
    main()
