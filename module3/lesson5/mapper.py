#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mapper.py"""

import sys


def perform_map():
    for line in sys.stdin:
        line = line.strip()
        values = line.split(',')
        if values[0] != 'VendorID' and \
            values[1] is not None and values[1] != '' and \
            values[1][:4] == '2020' and \
            values[9] is not None and values[9] != '':
            month = values[1][:7]
            tip_amount = values[13]
            payment_type = values[9]
            # print(f'{month}\t{tip_amount}')
            print('%s %s\t%s' % (month, payment_type, tip_amount))


if __name__ == '__main__':
    perform_map()
