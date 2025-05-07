#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 11:09:07 2023

@author: green-machine
"""


from pathlib import Path

import yaml
from core.constants import (DATA_CONSTRUCT_CAN_FORMER,
                            DATA_CONSTRUCT_CAN_FORMER_NOT_USED,
                            DATA_STATCAN_ARCHIVE)


def dump(file_path: Path, data: dict) -> None:
    with file_path.open('w') as f:
        yaml.dump(data, f, default_flow_style=False)


def main() -> None:

    file_path = 'combine_can_former.yaml'
    dump(file_path, DATA_CONSTRUCT_CAN_FORMER)

    file_path = 'combine_can_former_not_used.yaml'
    dump(file_path, DATA_CONSTRUCT_CAN_FORMER_NOT_USED)

    file_path = 'statcan_archive.yaml'
    dump(file_path, DATA_STATCAN_ARCHIVE)


if __name__ == '__main__':
    main()
