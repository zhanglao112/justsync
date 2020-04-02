#!/usr/bin/env python
# -*- coding: utf-8 -*-

# message type
BOOKING_MESSAGE_TYPE = 'booking'
FEE_MESSAGE_TYPE = 'parkingFee'
PAYMENT_MESSAGE_TYPE = 'pay'
CARD_MESSAGE_TYPE = 'card'

class ParkInfo(object):
    def __init__(self, park_id, spaces, vistor_spaces,
                 total_remaining_spaces, vip_remaining_spaces, visitor_remaining_spaces):
        self.park_id = park_id
        self.spaces = spaces
        self.vistor_spaces = visitor_spaces
        self.total_remaining_spaces = total_remaining_spaces
        self.vip_remaining_spaces = vip_remaining_spaces
        self.visitor_remaining_spaces = visitor_remaining_spaces

class ParkingInfo(object):
    __slots__ = ('park_id', 'parking_id', 'car_no', 'card_type',
                 'entry_time', 'entry_code', 'exit_time', 'exit_code',
                 'duration', 'fee')

    def __init__(self, park_id, parking_id, car_no, card_type):
        self.park_id = park_id
        self.parking_id = parking_id
        self.car_no = car_no
        self.card_type = card_type

    def set_entry(self, entry_time, entry_code = ''):
        self.entry_time = entry_time
        self.entry_code = entry_code

    def set_exit(self, exit_time, duration, fee, exit_code = ''):
        self.exit_time  = exit_time
        self.exit_code  = exit_code
        self.duration   = duration
        self.fee        = fee

class PaymentMessage(object):
    def __init__(self, park, car_no, parking, order_no, fee, pay_time, info):
        self.park = park
        self.car_no = car_no
        self.parking = parking
        self.order_no = order_no
        self.fee = fee
        self.pay_time = pay_time
        self.info = info

    def clone(self):
        obj = self.__class__.__new__(self.__class__)
        obj.__dict__ = self.__dict__.copy()
        return obj

class BookingMessage(object):
    def __init__(self, park, car_no, booking_id, start_time, end_time, status, slot , info):
        self.park = park
        self.car_no = car_no
        self.booking_id = booking_id
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.slot = slot
        self.info = info

    def clone(self):
        obj = self.__class__.__new__(self.__class__)
        obj.__dict__ = self.__dict__.copy()
        return obj


class FeeMessage(object):
    def __init__(self, park, car_no, parking, start_time, end_time, info):
        self.park = park
        self.car_no = car_no
        self.parking = parking
        self.start_time = start_time
        self.end_time = end_time
        self.info = info

class CardMessage(object):
    def __init__(self, park, client_name, car_no, phone, card_type, begin_time, end_time, operation, info):
        self.park = park
        self.client_name = client_name
        self.car_no = car_no
        self.phone = phone
        self.card_type = card_type
        self.begin_time = begin_time
        self.end_time = end_time
        self.operation = operation
        self.info = info

def nanning_common_charge(start_time, end_time):
    fee_max = 20
    fee_min = 5
    seconds_free = 10 * 60
    seconds_min = 2 * 60 * 60

    delta_time = end_time - start_time
    diff_days = delta_time.days
    diff_seconds = delta_time.seconds
    
    def fee_daily(days, diff_seconds):
        if days:
            fee = (diff_seconds / 3600) * 2
            if diff_seconds % 3600 <> 0:
                fee = fee + 2
            if fee > fee_max:
                return fee_max
            return fee

        if diff_seconds < seconds_free:
            return 0
        elif diff_seconds <= seconds_min:
            return fee_min
        else:
            other_seconds = diff_seconds - seconds_min
            fee = ((other_seconds) / 3600) * 2
            if other_seconds % 3600 <> 0:
                fee = fee + 2
            fee = 5 + fee
            if fee > fee_max:
                return fee_max
            return fee

    return diff_days * 20 + fee_daily(diff_days, diff_seconds)
