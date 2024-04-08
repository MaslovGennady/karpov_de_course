#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""reducer.py"""

import sys


payment_type_mapping = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }

def perform_reduce():
    current_month_and_payment = None
    current_tip_amount = 0
    current_tip_cnt = 0
    month_and_payment = ''

    for line in sys.stdin:
        line = line.strip()
        month_and_payment, tip_amount = line.split('\t')
        month, payment_type = month_and_payment.split(' ')
        payment_type = payment_type_mapping[int(payment_type)]
        month_and_payment = month + ',' + payment_type
        
        try:
            tip_amount = float(tip_amount)
        except ValueError:
            continue

        if current_month_and_payment == month_and_payment:
            current_tip_amount += tip_amount
            current_tip_cnt += 1
        else:
            if current_month_and_payment:
                tips_avg_amount = current_tip_amount / current_tip_cnt
                # print(f'{current_month}\t{tips_avg_amount}')
                print('%s,%s' % (current_month_and_payment, tips_avg_amount))

            current_month_and_payment = month_and_payment
            current_tip_amount = tip_amount
            current_tip_cnt = 1

    if current_month_and_payment == month_and_payment:
        tips_avg_amount = current_tip_amount / current_tip_cnt
        # print(f'{current_month}\t{tips_avg_amount}')
        print('%s,%s' % (current_month_and_payment, tips_avg_amount))


if __name__ == '__main__':
    perform_reduce()
