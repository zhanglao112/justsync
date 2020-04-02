#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import uuid
from services.plugin import BaseService
from database import handle_entry, handle_exit, handle_park
from database import handle_payment_ack, handle_booking_ack, handle_fee_ack
from common import ParkingInfo
from common import BOOKING_MESSAGE_TYPE, FEE_MESSAGE_TYPE, PAYMENT_MESSAGE_TYPE
from util import str2datetime, Prpcrypt
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log



PAMA_PARKING_INFO_TYPE = 'ParkingLotInfo'
PAMA_SPACE_TYPE        = 'space'
PAMA_ENTRY_TYPE        = 'inCar'
PAMA_EXIT_TYPE         = 'outCar'
PAMA_BOOKING_ACK_TYPE  = 'ReserveCarbarnResult'
PAMA_PAYMENT_ACK_TYPE  = 'PayResult'
PAMA_FEE_ACK_TYPE      = 'ParkPriceResult'

def _ack(srv, item, payload, result, msg = 'no message'):
    ack_id = ''

    try:
        data      = payload['Data']
        park_id   = data.get('parkId', data.get('ParkingLotId'))
        client_id = payload['ClientId']
        ack_id    = payload['Id']
        ack_dict = {'Id': str(uuid.uuid1()),
                    'DataType': 'Ack',
                    'ClientId': client_id,
                    'Data': {
                        'ackId': ack_id,
                        'result': result,
                        'msg': msg
                    }
        }
    except Exception, e:
        message = "[ACK] park [%s] ack id [%s] catch exception [%s]" % (park_id, ack_id, str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return

    #topic = item.config.get('topic_ack')
    topic = item.config.get('topic_prefix') + str(park_id)
    srv.mqttc.publish(topic, json.dumps(ack_dict))

    message = "[ACK] park [%s] ack id [%s]" % (park_id, ack_id)
    do_log(srv, item, "DEBUG", 'ACK', message)

def _send(srv, item, topic, payload_json):
    qos = 2

    try:
        _payload = json.dumps(payload_json)
    except Exception, e:
        message = "[SEND] payload [%s] catch exception [%s]" % (str(payload_json), str(e))
        do_log(srv, item, "WARN", LOG_T_INNER, message)
        return False

    srv.mqttc.publish(topic, _payload, qos)
    message = "[SEND] topic [%s] payload [%s] OK" % (topic, str(_payload))
    do_log(srv, item, "INFO", LOG_T_INNER, message)

    return True

def _parking_info_cb(srv, item, payload):
    _ack(srv, item, payload, True)
    return True

def _space_cb(srv, item, payload):
    try:
        data                     = payload['Data']
        park_id                  = int(data['parkId'])
        spaces                   = int(data['totalSpace'])
        total_remaining_spaces   = int(data['totRemainSpace'])
        vip_remaining_spaces     = int(data['vipRemainSpace'])
        visitor_remaining_spaces = int(data['vstRemainSpace'])
    except Exception, e:
        _ack(srv, item, payload, False)
        message = "[SPACE_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False
    
    notified = handle_park(srv, item, park_id, spaces, total_remaining_spaces,
                           vip_remaining_spaces, visitor_remaining_spaces)
    _ack(srv, item, payload, notified)
    return notified

def _entry_cb(srv, item, payload):
    try:
        data         = payload['Data']
        park_id      = int(data['parkId'])
        parking_id   = data['logId']
        car_no       = data['plateNumber']
        entry_code   = data['entranceCode']
        entry_time   = data['time']
        entry_time = str2datetime(entry_time)
        card_type    = (0 if int(data['carType']) == 0 else 3)
    except Exception, e:
        _ack(srv, item, payload, True)
        message = "[ENTRY_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    parking_info = ParkingInfo(park_id, parking_id, car_no, card_type)
    parking_info.set_entry(entry_time, entry_code)
    notified = handle_entry(srv, item, parking_info)
    _ack(srv, item, payload, notified)

    return notified
            
def _exit_cb(srv, item, payload):
    try:
        data         = payload['Data']
        parking_id   = data["inLogId"]
        car_no       = data["plateNumber"]
        exit_code    = data["entranceCode"]

        entry_time   = data["inTime"]
        entry_time   = str2datetime(entry_time)
        exit_time    = data["outTime"]
        exit_time    = str2datetime(exit_time)

        card_type    = (0 if int(data["carType"]) == 0 else 3)
        duration     = int(data["duration"])
        fee          = float(data["due"])
        park_id      = int(data["parkId"])
    except Exception, e:
        _ack(srv, item, payload, True)
        message = "[EXIT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    parking_info = ParkingInfo(park_id, parking_id, car_no, card_type)
    parking_info.set_exit(exit_time, duration, fee, exit_code)
    notified = handle_exit(srv, item , parking_info)
    _ack(srv, item, payload, notified)

    return notified

def _booking_cb(srv, item, payload):
    try:
        data       = payload['Data']
        mid_ack    = str(data['ackId'])
        pc         = Prpcrypt(PAMA_SECRET_KEY)
        mid_ack    = int(pc.decrypt(mid_ack))
        result_ack = data['result']
    except Exception, e:
        message = "[BOOKING_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    apply_state = ('pass' if result_ack else 'reject')
    notified = handle_booking_ack(srv, item, mid_ack, apply_state)
    return notified

def _payment_cb(srv, item, payload):
    try:
        data       = payload['Data']
        order_no   = data['ackId']
        pc         = Prpcrypt(PAMA_SECRET_KEY)
        order_no   = pc.decrypt(order_no)

        car_no     = data['plateNumber']
        park_id    = int(data['parkId'])
        parking_id = data['logId']
        result     = int(data['retCode'])
    except Exception, e:
        message = "[PAYMENT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    local_state = ('success' if result == 1 else 'fail')
    notified = handle_payment_ack(srv, item, park_id, parking_id, car_no, local_state, order_no)
    return notified

def _fee_cb(srv, item, payload):
    try:
        data         = payload['Data']
        car_no       = data['plateNumber']
        fee          = data['price']

        fee_time     = data['endTime']
        fee_time = str2datetime(fee_time)

        result       = int(data['retcode']) # FIXME: protocol doc is retCode
        park_id      = int(data['parkId'])
    except Exception, e:
        message = "[FEE_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    if result <> 1:
        return True

    notified = handle_fee_ack(srv, item, park_id, car_no, fee, fee_time)
    return notified

def do_booking(srv, item, m_booking):
    pc = Prpcrypt(PAMA_SECRET_KEY)
    mid = pc.encrypt(str(m_booking.booking_id))
    msg_json = { 'Id': mid,
                 'DataType': 'ReserveCarbarn',
                 'ClientId': 'none',
                 'Data': {
                     'orderId': mid,
                     'plateNum': m_booking.car_no,
                     'startTime': str(m_booking.start_time),
                     'endTime': str(m_booking.end_time),
                     'status': str(m_booking.status),
                     'carbarnNum': m_booking.slot
                 }
    }
    m_booking.info = item.config.get('topic_prefix') + str(m_booking.park)

    notified = _send(srv, item, m_booking.info, msg_json)
    return notified

def do_fee(srv, item, m_fee):
    msg_json = {
        'Id': str(uuid.uuid1()),
        'DataType': 'ParkPriceExcludePaid',
        'ClientId': 'none',
        'Data': {
            'logId': m_fee.parking,
            'plateNumber': m_fee.car_no,
            'startTime': str(m_fee.start_time),
            'endTime': str(m_fee.end_time)
        }
    }
    m_fee.info = item.config.get('topic_prefix') + str(m_fee.park)
    
    notified = _send(srv, item, m_fee.info, msg_json)
    return notified


def do_payment(srv, item, m_pay):
    pc = Prpcrypt(PAMA_SECRET_KEY)
    msg_id = pc.encrypt(str(m_pay.order_no))
    msg_json = {
        'Id': msg_id,
        'DataType': 'pay',
        'ClientId': 'none',
        'Data': {
            'logId': m_pay.parking,
            'plateNumber': m_pay.car_no,
            'type': 0,
            'money': m_pay.fee,
            'payId': msg_id
        }
    }
    m_pay.info = item.config.get('topic_prefix') + str(m_pay.park)

    notified = _send(srv, item, m_pay.info, msg_json)
    return notified

class PaMaService(BaseService):
    def __init__(self, srv, item):
        super(PaMaService, self).__init__(srv, item)
    
    def handle_client(self):
        _srv  = self.srv
        _item = self.item
        payload = _item.get('payload')

        try:
            payload = json.loads(payload)
            data_type = payload['DataType']
        except Exception, e:
            message = "[HANDLE_CLIENT] payload [%s] catch exception [%s]" % (str(payload), str(e))
            do_log(srv, item, "WARN", LOG_T_LOCAL, message)
            return False

        notified = True
        if data_type in self.client_handlers.keys():
            notified = self.client_handlers[data_type](_srv, _item, payload)

        return notified

PAMA_CLIENT_CALLBACKS = {
    PAMA_PARKING_INFO_TYPE: _parking_info_cb,
    PAMA_SPACE_TYPE: _space_cb,
    PAMA_ENTRY_TYPE: _entry_cb,
    PAMA_EXIT_TYPE: _exit_cb,
    PAMA_BOOKING_ACK_TYPE: _booking_cb,
    PAMA_PAYMENT_ACK_TYPE: _payment_cb,
    PAMA_FEE_ACK_TYPE: _fee_cb
}

PAMA_SERVER_CALLBACKS = {
    BOOKING_MESSAGE_TYPE: do_booking,
    FEE_MESSAGE_TYPE: do_fee,
    PAYMENT_MESSAGE_TYPE: do_payment
}

def plugin(srv, item):
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    service = PaMaService(srv, item)
    if service.is_from_client():
        service.initialize(True, PAMA_CLIENT_CALLBACKS)
        notified = service.handle_client()
    else:
        service.initialize(False, PAMA_SERVER_CALLBACKS)
        notified = service.handle_server()

    if not notified:
        srv.logging.warn("SERVICE=%s, RETURN FALSE.", item.service)
    return notified
