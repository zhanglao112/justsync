#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import datetime
from database import Pmc, Park, CurrentParking, HistoryParking, Booking, ClientBill
from common import PaymentMessage, BookingMessage, FeeMessage, CARD_MESSAGE_TYPE
from common import BOOKING_MESSAGE_TYPE, FEE_MESSAGE_TYPE, PAYMENT_MESSAGE_TYPE, CardMessage
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

def booking_cb(srv, item, payload):
    try:
        park       = payload['parkId']
        data       = payload['data']
        booking_id = data['booking_id']
        car_no     = data['car_no']
        start_time = datetime.datetime.fromtimestamp(data['start_time'])
        end_time   = datetime.datetime.fromtimestamp(data['end_time'])
        status     = data['status']
        slot       = data['slot']
    except Exception, e:
        message = "[BOOKING_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    if not item.addrs.has_key(park):
        message = "[BOOKING_CB] payload [%s] park [%s] invalid" % (str(payload), park)
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        # logging me
        return False

    info = item.addrs[park]
    m_booking = BookingMessage(park, car_no, booking_id, start_time, end_time, status, slot, info)
    return m_booking

def fee_cb(srv, item, payload):
    try:
        park       = payload['parkId']
        data       = payload['data']
        car_no     = data['car_no']
        parking_id = data['parking_id']
        start_time = datetime.datetime.fromtimestamp(data['start_time'])
        end_time   = datetime.datetime.fromtimestamp(data['end_time'])
    except Exception, e:
        message = "[FEE_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    if not item.addrs.has_key(park):
        message = "[FEE_CB] payload [%s] park [%s] invalid" % (str(payload), park)
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        # logging me
        return False

    info = item.addrs[park]
    m_fee = FeeMessage(park, car_no, parking_id, start_time, end_time, info)
    return m_fee

def pay_cb(srv, item, payload):
    try:
        park       = payload['parkId']
        data       = payload['data']
        order_no   = data['order_no']
        fee        = int(data['fee'])
        parking_id = data['parking_id']
        car_no     = data['car_no']
    except Exception, e:
        message = "[PAY_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    if not item.addrs.has_key(park):
        message = "[PAY_CB] payload [%s] park [%s] invalid" % (str(payload), park)
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    info = item.addrs[park]
    pay_time = datetime.datetime.now()
    m_pay = PaymentMessage(park, car_no, parking_id, order_no, fee, pay_time, info)
    return m_pay

def card_cb(srv, item, payload):
    try:
        park       = payload['parkId']
        data       = payload['data']
        name       = data['owner_name']
        car_no     = data['car_no']
        phone      = data['phone']
        card_type  = data['card_type']
        begin_time = datetime.datetime.fromtimestamp(data['start_time'])
        end_time   = datetime.datetime.fromtimestamp(data['end_time'])
        operation  = data['operation']
    except Exception, e:
        message = "[CARD_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    if not item.addrs.has_key(park):
        message = "[CARD_CB] payload [%s] park [%s] invalid" % (str(payload), park)
        do_log(srv, item, "WARN", LOG_T_SERVER, message)
        return False

    info = item.addrs[park]
    m_card = CardMessage(park, name, car_no, phone, card_type, begin_time, end_time, operation, info)
    return m_card

class BaseService(object):
    _message_callbacks = {
        BOOKING_MESSAGE_TYPE: booking_cb,
        FEE_MESSAGE_TYPE: fee_cb,
        PAYMENT_MESSAGE_TYPE: pay_cb,
        CARD_MESSAGE_TYPE: card_cb
    }

    def __init__(self, srv, item):
        self.srv = srv
        self.item = item
        self.client_handlers = {}
        self.server_handlers = {}

    def is_from_client(self):
        topic     = self.item.get('topic')
        sub_topic = self.srv.api_topic

        if topic.find(sub_topic) <> -1:
            return False
        else:
            return True

    def initialize(self, client, callbacks):
        if client:
            self.client_handlers = callbacks
        else:
            self.server_handlers = callbacks
    
    def handle_client(self):
        raise NotImplementedError()

    def _loads(self):
        payload = self.item.get('payload')

        try:
            payload   = json.loads(payload)
            data_type = payload['dataType']
        except Exception, e:
            message = "[_LOADS] payload [%s] catch exception [%s]" % (str(payload), str(e))
            do_log(srv, item, "WARN", LOG_T_SERVER, message)
            return False

        if data_type not in self._message_callbacks.keys():
            return False
        message =  self._message_callbacks[data_type](self.srv, self.item, payload)
        return (data_type, message)

    def handle_server(self):
        notified = False
        
        data_type , message = self._loads()
        if message and data_type in self.server_handlers.keys():
            notified = self.server_handlers[data_type](self.srv, self.item, message)

        return notified

    def add_handler(self, client, key, handler):
        pass

class CommonService(BaseService):
    def __init__(self, srv, item):
        super(CommonService, self).__init__(srv, item)

    def _client_loads(self):
        payload = self.item.get('payload')

        if payload.find('\\') <> -1:
            payload = payload.replace('\\', '\\\\')

        json_list = []
        str_list = []
        plist = payload.split('}{')
        if len(plist) > 1:
            for i in range(len(plist)):
                if i == 0:
                    str_list.append(plist[i] + '}')
                elif i == len(plist) - 1:
                    str_list.append('{' + plist[i])
                else:
                    str_list.append('{' + plist[i] + '}')
        else:
            str_list.append(plist[0])
        try:
            for i in str_list:
                json_list.append(json.loads(i))
        except Exception, e:
            message = "[_CLIENT_LOADS] payload [%s] catch exception [%s]" % (str(payload), str(e))
            do_log(self.srv, self.item, "WARN", LOG_T_LOCAL, message)
        
        return json_list

    def handle_client(self):
        notified = True

        json_list = self._client_loads()
        for data in json_list:
            #self.item.data = data
            table_list = data.keys()
            for table_name in table_list:
                if table_name in self.client_handlers.keys():
                    notified = self.client_handlers[table_name](self.srv, self.item, data)
        return notified

 
