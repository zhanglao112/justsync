#!/usr/bin/env python
# -*- coding: utf-8 -*-

from services.plugin import CommonService
from database import handle_entry, handle_exit
from database import handle_payment_ack, handle_ketuo_payment, handle_fee_ack
from database import handle_ketuo_card
from common import FEE_MESSAGE_TYPE, PAYMENT_MESSAGE_TYPE, CARD_MESSAGE_TYPE
from common import ParkingInfo
from common import nanning_common_charge

from util import str2datetime
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

KETUO_ENTRY_TYPE = 't_carcome'
KETUO_EXIT_TYPE = 't_pay_detail'
KETUO_PAYMENT_ACK_TYPE = 't_prepaid_fee_LocalAck'

def _entry_cb(srv, item, payload):
    try:
        data       = payload[KETUO_ENTRY_TYPE]
        event_type = data['eventType']
        fields     = data['data']

        car_no     = fields['carPlateNum']
        entry_time = fields['comeTime']
        entry_time = str2datetime(entry_time)
        entry_code = fields['caremaId']
        parking_id = str(fields['id'])
        park_id    = int(data['parkID'])
        card_type  = int(fields['carType'])
    except Exception, e:
        message = "[ENTRY_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    if event_type.upper() <> 'INSERT':
        return True

    parking_info = ParkingInfo(park_id, parking_id, car_no, card_type)
    parking_info.set_entry(entry_time, entry_code)
    notified = handle_entry(srv, item, parking_info)

    return notified

def _exit_cb(srv, item, payload):
    try:
        data       = payload[KETUO_EXIT_TYPE]
        park_id    = int(data['parkID'])
        event_type = data['eventType']
        fields     = data['data']

        car_no     = fields['carPatenum']
        parking_id = fields['id']
        entry_time = fields['comeTime']
        entry_time = str2datetime(entry_time)
        exit_time = fields['outTime']
        exit_time = str2datetime(exit_time)
        exit_code = fields['outPlace']
        duration = (exit_time - entry_time).total_seconds() / 60

        pay_money = float(fields['payMoney'])
        pre_money = float(fields['preMoney'])
        ticket    = float(fields['ticket'])
        total_money = float(fields['totalMoney'])
        in2out_money = float(fields['InToOutMoney'])
        fee = in2out_money if in2out_money >= total_money else total_money
        if pre_money > fee:
            fee = pre_money
        elif ticket > fee:
            fee = ticket
        elif pay_money > fee:
            fee = pay_money
    except Exception, e:
        message = "[EXIT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    #TODO insert or update or delete
    parking_info = ParkingInfo(park_id, parking_id, car_no, 0)
    parking_info.set_exit(exit_time, duration, fee, exit_code)
    notified = handle_exit(srv, item, parking_info)

    return notified

def _payment_cb(srv, item, payload):
    try:
        data         = payload[KETUO_PAYMENT_ACK_TYPE]
        fields       = data['data']
        car_no       = fields['carplatenum']
        parking      = str(fields['comecarId'])
        park_id      = int(data['parkID'])
    except Exception, e:
        message = "[PAYMENT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    notified = handle_payment_ack(srv, item, park_id, parking, car_no, 'success')
    return notified

def do_booking(srv, item, m_booking):
    return False

def do_fee(srv, item, m_fee):
    fee = nanning_common_charge(m_fee.start_time, m_fee.end_time)
    fee = fee * 100
    notified = handle_fee_ack(srv, item, m_fee.park, m_fee.car_no, fee, m_fee.end_time)

    return notified

def do_payment(srv, item, m_pay):
    notified = handle_ketuo_payment(srv, item, m_pay)

    return notified

def do_card(srv, item, m_card):
    notified = handle_ketuo_card(srv, item, m_card)

    return notified
class KeTuoService(CommonService):
    def __init__(self, srv, item):
        super(KeTuoService, self).__init__(srv, item)

KETUO_CLIENT_CALLBACKS = {
    KETUO_ENTRY_TYPE: _entry_cb,
    KETUO_EXIT_TYPE: _exit_cb,
    KETUO_PAYMENT_ACK_TYPE: _payment_cb
}

KETUO_SERVER_CALLBACKS = {
    FEE_MESSAGE_TYPE: do_fee,
    PAYMENT_MESSAGE_TYPE: do_payment,
    CARD_MESSAGE_TYPE: do_card
}

def plugin(srv, item):
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    service = KeTuoService(srv, item)
    if service.is_from_client():
        service.initialize(True, KETUO_CLIENT_CALLBACKS)
        notified = service.handle_client()
    else:
        service.initialize(False, KETUO_SERVER_CALLBACKS)
        notified = service.handle_server()

    if not notified:
        srv.logging.warn("SERVICE=%s, RETURN FALSE.", item.service)

    return notified

    



        
    
