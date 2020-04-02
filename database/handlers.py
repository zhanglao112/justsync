#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from peewee import MySQLDatabase
from models import Pmc
from models import Park
from models import CurrentParking
from models import HistoryParking
from models import Booking
from models import ClientBill
from models import PmcCar

from models import NanHuParkingInfo
from models import LiFangUserInfo
from models import KeTuoPrepaidFee, KeTuoCarCard, KeTuoCarCardSub

from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

class BackupDatabase(object):
    __slots__ = ('database',)

    def __init__(self, srv, info):
        db_host   = srv.cfg.db_host
        db_port   = srv.cfg.db_port
        db_user   = srv.cfg.db_user
        db_passwd = srv.cfg.db_pass


        self.database = MySQLDatabase(info, **{'host': db_host, 'password': db_passwd, 'user': db_user})
    
    def close(self):
        self.database.close()


def handle_entry(srv, item, parking_info):
    _srv = srv
    notified = True

    try:
        with _srv.database:
            try:
                park = (Park
                        .select(Park, Pmc)
                        .join(Pmc)
                        .where(Park.id == parking_info.park_id)).get()
            except Park.DoesNotExist:
                message = "[ENTRY] park [%s] does not exist" % parking_info.park_id
                do_log(srv, item, "WARN", LOG_T_LOCAL, message)
                return True

            defaults = dict(park=park,
                            pmc=park.pmc,
                            parking_id=parking_info.parking_id,
                            entry_time=parking_info.entry_time,
                            entry_code=parking_info.entry_code,
                            card_type=parking_info.card_type)
            current_parking, created = CurrentParking.get_or_create(car_no=parking_info.car_no,
                                                                    defaults=defaults)
            parking = str(current_parking.id)
            if created:
                history_parking = HistoryParking.create(pmc=park.pmc,
                                                        park=park,
                                                        parking=parking,
                                                        car_no=parking_info.car_no,
                                                        entry_time=parking_info.entry_time,
                                                        entry_code=parking_info.entry_code,
                                                        card_type=parking_info.card_type)
            else:
                if parking_info.entry_time > current_parking.entry_time:
                    current_parking.pmc = park.pmc
                    current_parking.park = park
                    current_parking.parking_id = parking_info.parking_id
                    current_parking.entry_time = parking_info.entry_time
                    current_parking.entry_code = parking_info.entry_code
                    current_parking.card_type = parking_info.card_type
                    current_parking.save()

                    query = HistoryParking.update(pmc=park.pmc,
                                                  park=park,
                                                  card_type=parking_info.card_type,
                                                  entry_code=parking_info.entry_code,
                                                  entry_time=parking_info.entry_time).where((HistoryParking.parking == parking) &
                                                                                            (HistoryParking.car_no == parking_info.car_no))
                    query.execute()

            query = Booking.update(parking=parking,
                                   reality_start=parking_info.entry_time,
                                   parking_state='parking').where((Booking.park == parking_info.park_id) &
                                                                  (Booking.car_no == parking_info.car_no) &
                                                                  (Booking.apply_state == 'pass') &
                                                                  ~ (Booking.parking_state << ['leaved']))
            query.execute()
    except Exception, e:
        message = "[ENTRY] park [%s] car [%s] catch excetion [%s]" % (parking_info.park_id, parking_info.car_no, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        notified = False

    return notified

def handle_exit(srv, item, parking_info):
    _srv = srv
    
    notified = True
    try:
        with _srv.database:
            try:
                current_parking = CurrentParking.get(CurrentParking.park == parking_info.park_id,
                                                     CurrentParking.car_no == parking_info.car_no)
            except CurrentParking.DoesNotExist:
                message = "[EXIT] park [%s] car [%s] does not exist" % (parking_info.park_id, parking_info.car_no)
                do_log(srv, item, "WARN", LOG_T_LOCAL, message)
                return True

            parking = str(current_parking.id)
            current_parking.delete_instance()
            query = HistoryParking.update(exit_time=parking_info.exit_time,
                                          exit_code=parking_info.exit_code,
                                          duration=parking_info.duration,
                                          fee=parking_info.fee).where((HistoryParking.park == parking_info.park_id) &
                                                                      (HistoryParking.parking == parking) &
                                                                      (HistoryParking.car_no == parking_info.car_no))
            query.execute()
            query = Booking.update(reality_end=parking_info.exit_time,
                                   parking_state='leaved').where((Booking.park == parking_info.park_id) &
                                                                 (Booking.parking == parking) &
                                                                 (Booking.car_no == parking_info.car_no))
            query.execute()
    except Exception, e:
        message = "[EXIT] park [%s] car [%s] catch excetion [%s]" % (parking_info.park_id, parking_info.car_no, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        notified = False
    return notified

def handle_payment_ack(srv, item, park_id, parking, car_no, local_state, order_no = None):
    _srv = srv

    try:
        with _srv.database:
            if order_no is None:
                query = ClientBill.update(local_state=local_state).where((ClientBill.parking_id == parking) &
                                                                         (ClientBill.park == park_id) &
                                                                         (ClientBill.car_no == car_no))
            else:
                query = ClientBill.update(local_state=local_state).where((ClientBill.order_no == order_no) &
                                                                         (ClientBill.park == park_id) &
                                                                         (ClientBill.car_no == car_no))
            query.execute()
    except Exception, e:
        message = "[PAYMENT_ACK] park [%s] car [%s] catch exception [%s]" % (park_id, car_no, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        return False

    return True

def handle_booking_ack(srv, item, booking_id, apply_state):
    _srv = srv

    try:
        with _srv.database:
            query = Booking.udpate(apply_state=apply_state).where(Booking.id == booking_id)
            query.execute()
    except Exception, e:
        message = "[BOOKING_ACK] booking id [%s] catch exception [%s]" % (booking_id, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        return False

    return True

def handle_fee_ack(srv, item, park_id, car_no, fee, fee_time):
    _srv = srv

    try:
        with _srv.database:
            query = CurrentParking.update(fee=fee,
                                          fee_time=fee_time).where((CurrentParking.park == park_id) &
                                                                   (CurrentParking.car_no == car_no))
            query.execute()
    except Exception, e:
        message = "[FEE_ACK] park [%s] car [%s] catch exception [%s]" % (park_id, car_no, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        return False

    return True

def handle_park(srv, item, park_id, spaces, total_remaining_spaces,
                vip_remaining_spaces = 0, visitor_remaining_spaces = 0):
    _srv = srv

    try:
        with _srv.database:
            query = Park.update(spaces=spaces,
                                total_remaining_spaces=total_remaining_spaces,
                                vip_remaining_spaces=vip_remaining_spaces,
                                visitor_remaining_spaces=visitor_remaining_spaces).where(Park.id == park_id)
            query.execute()
    except Exception, e:
        message = "[PARK_SPACE] park [%s] catch exception [%s]" % (park_id, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        return False

    return True

def handle_park_heartbeat(srv, item, park_id, is_online = None, last_heartbeat_time = None):
    _srv = srv

    try:
        with _srv.database:
            if is_online is None:
                query = Park.update(last_heartbeat_time=last_heartbeat_time).where(Park.id == park_id)
            else:
                query = Park.update(is_online=is_online).where(Park.id == park_id)

            query.execute()
    except Exception, e:
        message = "[PARK_HEARTBEAT] park [%s] catch exception [%s]" % (park_id, str(e))
        do_log(srv, item, "ERROR", LOG_T_DB, message)
        # loggine me
        return False

    return True

def handle_lifang_booking(srv, item, m_booking):
    backup_db = BackupDatabase(srv, m_booking.info)
    models = (LiFangUserInfo,)

    SOME_USER = 'some_user'
    charge_rule_id = 1001
    with backup_db.database.bind_ctx(models):
        try:
            if m_booking.status == 0:
                defaults = dict(user_no=SOME_USER,
                                name='some_'+ m_booking.car_no,
                                car_no=m_booking.car_no,
                                charge_rule_id=charge_rule_id,
                                begin_time=m_booking.start_time,
                                end_time=m_booking.end_time,
                                create_date=datetime.datetime.now())
                user_info, created = LiFangUserInfo.get_or_create(car_no=m_booking.car_no,
                                                                  user_no=SOME_USER,
                                                                  defaults=defaults)
                if not created:
                    query = LiFangUserInfo.update(begin_time=m_booking.start_time,
                                                  end_time=m_booking.end_time).where((LiFangUserInfo.car_no == m_booking.car_no) &
                                                                                     (LiFangUserInfo.user_no == SOME_USER))
                    query.execute()

            else:
                query = LiFangUserInfo.delete().where((LiFangUserInfo.car_no == m_booking.car_no) &
                                                      (LiFangUserInfo.user_no == 'some_user'))
                query.execute()
        except Exception, e:
            backup_db.close()
            message = "[DO_BOOKING] park [%s] car [%s] catch exception [%s]" % (m_booking.park, m_booking.car_no, str(e))
            do_log(srv, item, "WARN", LOG_T_DB, message)
            return False
    backup_db.close()

    return True

def handle_nanhu_payment(srv, item, m_pay):
    backup_db = BackupDatabase(srv, m_pay.info)
    models = (NanHuParkingInfo,)

    with backup_db.database.bind_ctx(models):
        try:
            query = NanHuParkingInfo.update(is_center_pay=1,
                                            pay_time=m_pay.pay_time,
                                            pay_fee=m_pay.fee / 100.0).where(NanHuParkingInfo.parking_id == m_pay.parking)
            query.execute()
        except Exception, e:
            backup_db.close()
            message = "[HANDLE_NANHU_PAYMENT] park [%s] car [%s] catch exception [%s]" % (m_pay.park, m_pay.car_no, str(e))
            do_log(srv, item, "ERROR", LOG_T_DB, message)
            return False

    backup_db.close()
    return True

def handle_ketuo_payment(srv, item, m_pay):
    backup_db = BackupDatabase(srv, m_pay.info)
    models = (KeTuoPrepaidFee,)

    car_no = m_pay.car_no[-6:]
    with backup_db.database.bind_ctx(models):
        try:
            # TODO: entry_time ?
            defaults = dict(pay_fee=m_pay.fee,
                            pay_time=m_pay.pay_time,
                            pay_channel=2,
                            amount=m_pay.fee,
                            casher=199,
                            car_serial=0,
                            serial_type=10,
                            is_paid=1)
            prepaid_fee, created = KeTuoPrepaidFee.get_or_create(car_no=car_no,
                                                                 parking_id=m_pay.parking,
                                                                 defaults=defaults)
            if not created:
                # loggine me
                pass
        except Exception, e:
            backup_db.close()
            message = "[HANDLE_KETUO_PAYMENT] park [%s] car [%s] catch exception [%s]" % (m_pay.park, m_pay.car_no, str(e))
            do_log(srv, item, "ERROR", LOG_T_DB, message)
            return False

    backup_db.close()
    return True

def handle_ketuo_card(srv, item, m_card):
    backup_db = BackupDatabase(srv, m_card.info)
    models = (KeTuoCarCard, KeTuoCarCardSub)

    with backup_db.database.bind_ctx(models):
        try:
            SOME_CREATER = 'some'
            provice = m_card.car_no[:-6] # FIXME
            car_no = m_card.car_no[-6:]
            if m_card.operation == 0: # ADD
                now = datetime.datetime.now()
                defaults = dict(phone=m_card.phone,
                                card_type=2,
                                product_id=0,
                                begin_time=m_card.begin_time,
                                end_time=m_card.end_time,
                                card_state=1,
                                remark=SOME_CREATER,
                                create_user=1,
                                create_time=now,
                                audit_user=1,
                                audit_time=now,
                                car_no_count=1)
                card, created = KeTuoCarCard.get_or_create(client_name=m_card.client_name,
                                                           card_name=SOME_CREATER,
                                                           room_id=SOME_CREATER,
                                                           defaults=defaults)
                if created:
                    sub = KeTuoCarCardSub.create(card_id=card.id,
                                                 provice=provice,
                                                 car_no=car_no,
                                                 car_brand='1',
                                                 state=1,
                                                 remark=SOME_CREATER)
            elif m_card.operation == 1 or m_card.operation == 2: # update or delete
                try:
                    sub = KeTuoCarCardSub.get(KeTuoCarCardSub.remark == SOME_CREATER,
                                              KeTuoCarCardSub.provice == provice,
                                              KeTuoCarCardSub.car_no == car_no,
                                              KeTuoCarCardSub.state == 1)
                except KeTuoCarCardSub.DoesNotExist:
                    message = "[HANDLE_KETUO_CARD] operation [1] park [%s] car [%s] does not exist" % (m_card.park, m_card.car_no)
                    do_log(srv, item, "WARN", LOG_T_SERVER, message)
                    return False
                card_id = sub.card_id

                if m_card.operation == 1: # update
                    query = KeTuoCarCard.update(phone=m_card.phone,
                                                begin_time=m_card.begin_time,
                                                end_time=m_card.end_time).where(KeTuoCarCard.id == card_id)
                    query.execute()
                else: # delete
                    sub.delete_instance()
                    query = KeTuoCarCard.delete().where(KeTuoCarCard.id == card_id)
                    query.execute()
            else:
                pass
        except Exception, e:
            backup_db.close()
            message = "[HANDLE_KETUO_CARD] operation [%s] park [%s] car [%s] catch excetion [%s]" % (m_card.operation, m_card.park, m_card.car_no, str(e))
            do_log(srv, item, "ERROR", LOG_T_DB, message)
            return False

    backup_db.close()
    return True
