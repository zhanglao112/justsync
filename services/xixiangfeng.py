#!/usr/bin/env python
# -*- coding: utf-8 -*-

from services.plugin import CommonService
from common import ParkingInfo
from database import handle_entry, handle_exit
from util import str2datetime
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

XIXIANGFENG_PARKING_TABLE = 'parked'

def _parking_cb(srv, item, payload):
    try:
        data       = payload[XIXIANGFENG_PARKING_TABLE]
        event_type = data['eventType']
        fields     = data['data']
        car_no     = fields['car_cp']

        entry_time = fields['in_time']
        entry_time = str2datetime(entry_time)

        entry_code = fields['in_sbname']
        parking_id = str(fields['parked_id'])
        park_id    = int(data['parkID'])
        card_type  = 2 if fields['card_kind'] == '月租卡' else 0
    except Exception, e:
        message = "[PARKING_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    notified = True
    parking_info = ParkingInfo(park_id, parking_id, car_no, card_type)
    if event_type.upper() == 'INSERT':
        parking_info.set_entry(entry_time, entry_code)
        notified = handle_entry(srv, item, parking_info)
    elif event_type.upper() == 'UPDATE' and fields['out_time'] <> 'null':
        try:
            exit_time = fields['out_time']
            exit_time = str2datetime(exit_time)

            exit_code = fields['out_sbname']
            duration = (exit_time - entry_time).total_seconds() / 60
            fee = float(fields['CentreReceiveMoney']) * 10
        except Exception, e:
            message = "[PARKING_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
            do_log(srv, item, "WARN", LOG_T_LOCAL, message)
            return False

        parking_info.set_exit(exit_time, duration, fee, exit_code)
        notified = handle_exit(srv, item, parking_info)
    else:
        pass

    return notified

def do_booking(srv, item, m_booking):
    return False

def do_fee(srv, item, m_fee):
    return False

def do_payment(srv, item, m_pay):
    return False

class XiXiangFengService(CommonService):
    def __init__(self, srv, item):
        super(XiXiangFengService, self).__init__(srv, item)

XIXIANGFENG_CLIENT_CALLBACKS = {
    XIXIANGFENG_PARKING_TABLE: _parking_cb
}

def plugin(srv, item):
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    service = XiXiangFengService(srv, item)
    if service.is_from_client():
        service.initialize(True, XIXIANGFENG_CLIENT_CALLBACKS)
        notified = service.handle_client()
    else:
        notified = True

    if not notified:
        srv.logging.warn("SERVICE=%s, RETURN FALSE.", item.service)

    return notified
