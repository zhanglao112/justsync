#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from peewee import MySQLDatabase
from services.plugin import CommonService
from database import handle_lifang_booking
from database import handle_entry, handle_exit, handle_park
from common import ParkingInfo
from common import BOOKING_MESSAGE_TYPE
from util import str2datetime
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

LIFANG_PARKING_INFO_TYPE = 'tc_parkinglotinfo'
LIFANG_ENTRY_TYPE = 'tc_usercrdtm_in'
LIFANG_EXIT_TYPE = 'tc_usercrdtm_out'

def _entry_cb(srv, item, payload):
    try:
        data         = payload[LIFANG_ENTRY_TYPE]
        event_type   = data['eventType']
        fields       = data['data']
        car_no       = fields['CarCode']
        entry_time   = fields['Crdtm']
        entry_time   = str2datetime(entry_time)
        entry_code   = fields['ChannelID']
        parking_id   = str(fields['RecordID'])
        park_id      = int(data['parkID'])
        card_type    = 2
    except Exception, e:
        message = "[ENTRY_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    notified = True
    if event_type.upper() <> 'DELETE':
        parking_info = ParkingInfo(park_id, parking_id, car_no, card_type)
        parking_info.set_entry(entry_time, entry_code)
        notified = handle_entry(srv, item, parking_info)
    else:
        pass

    return notified

def _exit_cb(srv, item):
    try:
        data         = item.data[LIFANG_EXIT_TYPE]
        event_type   = data['eventType']
        fields       = data['data']

        car_no       = fields['CarCode']
        entry_time   = fields['InTime']
        entry_time   = str2datetime(entry_time)
        exit_time    = fields['OutEventTime']
        exit_time    = str2datetime(exit_time)
        exit_code    = ''
        duration     = fields['StopTime']
        fee          = float(fields['Amount'])

        parking_id   = 'YY_UNUSED'
        park_id      = int(data['parkID'])
    except Exception, e:
        message = "[EXIT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    notified = True
    if event_type.upper() <> 'DELETE':
        parking_info = ParkingInfo(park_id, parking_id, car_no, 2)
        parking_info.set_exit(exit_time, duration, fee, exit_code)
        notified = handle_exit(srv, item, parking_info)

        # TODO delete userInfo
    else:
        pass

    return notified

def _space_cb(srv, item, payload):
    try:
        data                    = payload[LIFANG_PARKING_INFO_TYPE]
        event_type              = data['eventType']
        fields                  = data['data']
        total_spaces            = int(fields['CountCw'])
        total_remainning_spaces = int(fields['PrepCw'])
        park_id                 = int(data['parkID'])
    except Exception, e:
        message = "[SPACE_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    notified = True
    if event_type.upper() == 'DELETE' or event_type.upper() == 'INSERT':
        return notified

    notified = handle_park(srv, item, park_id, total_spaces, total_remaining_spaces)
    return notified

def do_booking(srv, item, m_booking):
    notified = handle_lifang_booking(srv, item, m_booking)

    return notified

def do_fee(srv, item, m_fee):
    return False

def do_payment(srv, item, m_pay):
    return False

class LiFangService(CommonService):
    def __init__(self, srv, item):
        super(LiFangService, self).__init__(srv, item)

LIFANG_CLIENT_CALLBACKS = {
    LIFANG_PARKING_INFO_TYPE: _space_cb,
    LIFANG_ENTRY_TYPE: _entry_cb,
    LIFANG_EXIT_TYPE: _exit_cb
}

LIFANG_SERVER_CALLBACKS = {
    BOOKING_MESSAGE_TYPE: do_booking,
}

def plugin(srv, item):
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    service = LiFangService(srv, item)
    if service.is_from_client():
        service.initialize(True, LIFANG_CLIENT_CALLBACKS)
        notified = service.handle_client()
    else:
        service.initialize(False, LIFANG_SERVER_CALLBACKS)
        notified = service.handle_server()

    if not notified:
        srv.logging.warn("SERVICE=%s, RETURN FALSE.", item.service)

    return notified
