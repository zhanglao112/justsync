#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from database import ClientBill
from common import PaymentMessage

class Item(object):
    def __init__(self, service, todo = None):
        self.service = service
        self.todo = todo

def handle_payment(srv=None):
    payment_list = []

    try:
        with srv.database:
            for bill in ClientBill.select().where(ClientBill.is_paid == 1, ClientBill.refunded == 0, ClientBill.state == 'success', ClientBill.local_state == 'loading', ClientBill.park.in_(srv.park_handler.keys())):
                if bill.pay_time is None:
                    continue

                pay_time = datetime.datetime.fromtimestamp(float(bill.pay_time))
                m_pay = PaymentMessage(bill.park, bill.car_no, bill.parking_id, bill.order_no,
                                       bill.amount, pay_time, srv.park_handler[bill.park]['info'])
                payment_list.append(m_pay)
    except Exception, e:
        srv.logging.error("[FUNCTIONS:HANDLE_PAYMENT] catch exception [%s]", str(e))
        pass

    for m_pay in payment_list:
        if m_pay.park not in srv.park_handler.keys():
            continue

        item = Item('FUNCTIONS:HANDLE_PAYMENT')
        srv.park_handler[m_pay.park]['module'].do_payment(srv, item, m_pay)
                               
                               
                               
                               
                               
            
        
        
        
    
    
