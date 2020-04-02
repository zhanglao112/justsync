#!/usr/bin/env python
# -*- coding: utf-8 -*-

from playhouse.pool import PooledMySQLDatabase
from peewee import *

database = PooledMySQLDatabase(None)

class BaseModel(Model):
    class Meta:
        database = database

class Pmc(BaseModel):
    name = CharField()

    class Meta:
        table_name = 'pmcs'

class Park(BaseModel):
    pmc = ForeignKeyField(Pmc, backref='parks')
    spaces = IntegerField(column_name='parking_lots', null=True)
    vistor_spaces = IntegerField(column_name='parking_visitor_lots', null=True)
    total_remaining_spaces = IntegerField(column_name='total_remain_lots', null=True)
    vip_remaining_spaces = IntegerField(column_name='vip_remain_lots', null=True)
    visitor_remaining_spaces = IntegerField(column_name='visitor_remain_lots', null=True)
    is_online = IntegerField(null=True)
    last_heartbeat_time = DateTimeField(null=True)

    class Meta:
        table_name = 'pmc_parks'

class CurrentParking(BaseModel):
    pmc = ForeignKeyField(Pmc, backref='current_parkings')
    park = ForeignKeyField(Park, backref='current_parkings')
    parking_id = CharField(index=True)
    car_no = CharField(column_name='car_id', null=True)
    card_type = IntegerField(null=True)
    entry_time = DateTimeField(null=True)
    entry_code = CharField(null=True)
    fee = FloatField(default=0)
    fee_time = DateTimeField(null=True)
    pay_fee = FloatField(default=0)
    last_payment_time = DateTimeField(null=True)

    class Meta:
        table_name = 'pmc_curr_parking'

class HistoryParking(BaseModel):
    pmc = ForeignKeyField(Pmc, backref='parking_entities')
    park = ForeignKeyField(Park, backref='parking_entities')
    parking = CharField(column_name='parking_id', index=True, null=True)
    car_no = CharField(column_name='car_id')
    card_type = IntegerField(null=True)
    entry_code = CharField(null=True)
    entry_time = DateTimeField(null=True)
    exit_code = CharField(null=True)
    exit_time = DateTimeField(null=True)
    duration = IntegerField(default=0)
    fee = FloatField(default=0)
    pay_fee = FloatField(default=0)
    pay_way = CharField(null=True)

    class Meta:
        table_name = 'pmc_parking_entity'

class Booking(BaseModel):
    park = ForeignKeyField(Park, backref='park_bookings')
    parking = CharField(column_name='parking_id', null=True)
    car_no = CharField(null=True)
    reality_start = DateTimeField(null=True)
    reality_end = DateTimeField(null=True)
    apply_state = CharField(default='applying')
    parking_state = CharField(default='not_parking')

    class Meta:
        table_name = 'pmc_park_booking'

class ClientBill(BaseModel):
    #park = ForeignKeyField(Park, related_name='parking_entities')
    park = IntegerField(column_name='park_id', null=True)
    parking_id = CharField(column_name='parking_id', null=True)
    order_no = CharField(null=True)
    car_no  = CharField(null=True)
    amount = FloatField(default=0)
    is_paid = SmallIntegerField(column_name='paid', default=0)
    pay_time = CharField(column_name='time_paid', null=True)
    refunded = SmallIntegerField(default=0)
    state = CharField(default='applying')
    local_state = CharField(column_name='state_local',default='loading')

    class Meta:
        table_name = 'client_bill_records'

class PmcCar(BaseModel):
    park = ForeignKeyField(Park, backref='pmc_cars', null=True)
    car_no = CharField(null=True)
    owner_name = CharField(null=True)
    owner_link = CharField(null=True)
    car_type = IntegerField(null=True, default=0)
    car_size_type = IntegerField(null=True, default=0)
    card_start_time = DateTimeField(null=True)
    card_end_time = DateTimeField(column_name='car_end_time', null=True)

    class Meta:
        table_name = 'pmc_cars'



# plugin models
class NanHuParkingInfo(Model):
    parking_id = IntegerField(column_name='ID', null=True)
    total_fee = FloatField(column_name='TotalCost', null=True)
    final_fee = FloatField(column_name='FinalCost', null=True)
    is_center_pay = SmallIntegerField(column_name='IsCenterPay', null=True)
    pay_time = DateTimeField(column_name='CenterPayTime', null=True)
    pay_fee = FloatField(column_name='CenterPay', null=True)

    class Meta:
        table_name = 'ParkingInfo'

class LiFangUserInfo(Model):
    user_id     = IntegerField(column_name='RecordID', null=True)
    user_no     = CharField(column_name='UserNo', null=True)
    name        = CharField(column_name='UserName', null=True)
    car_no      = CharField(column_name='CarCode', null=True)
    car_no1      = CharField(column_name='CarCode1', null=True)
    car_no2      = CharField(column_name='CarCode2', null=True)
    charge_rule_id = IntegerField(column_name='ChargeRuleID', null=True)
    user_property = IntegerField(column_name='UserPropertiy', default=0)
    begin_time    = DateTimeField(column_name='Bgndt', null=True)
    end_time      = DateTimeField(column_name='Enddt', null=True)
    car_label     = CharField(column_name='CarLabel', null=True)
    car_color     = CharField(column_name='CarColor', null=True)
    phone         = CharField(column_name='UserTel', null=True)
    address       = CharField(column_name='UserAddress', null=True)
    note          = CharField(column_name='UserMemo', null=True)
    create_people = CharField(column_name='CreatePeople', default='some_creater')
    create_date   = CharField(column_name='CreateDate', null=True)

    class Meta:
        table_name = 'tc_userinfo'


class KeTuoPrepaidFee(Model):
    car_no = CharField(column_name='carplatenum')
    parking_id = IntegerField(column_name='comecarId', default=0)
    pay_fee = IntegerField(column_name='paiddata', default=0)
    pay_time = DateTimeField(column_name='paidTime')
    pay_channel = IntegerField(column_name='payType', default=1)
    amount = IntegerField(column_name='totalMoney', null=True, default=0)
    entry_time = DateTimeField(column_name='InCarTime', null=True)
    casher = IntegerField(default=0)
    car_serial = CharField(column_name='carserial')
    pay_type = IntegerField(column_name='idType', default=0)
    serial_type = IntegerField(column_name='serialType', null=True, default=0)
    is_paid = SmallIntegerField(column_name='IsAlllPay', null=True, default=1)

    class Meta:
        table_name = 't_prepaid_fee'
    
class KeTuoCarCard(Model):
    card_name = CharField(column_name='CardName')
    client_name = CharField(column_name='Name')
    phone = CharField(column_name='Tel')
    room_id = CharField(column_name='roomId')
    card_type = IntegerField(column_name='CarType', default=0)
    product_id = IntegerField(column_name='CartypeSub', default=0)
    lots_count = IntegerField(column_name='UsePPlace', default=1)
    begin_time = DateTimeField(column_name='ValidForm')
    end_time = DateTimeField(column_name='ValidTo')
    valid_value = IntegerField(column_name='ValidValue', default=0)
    card_state = IntegerField(column_name='CardState', default=0)
    remark = CharField(column_name='Remark')
    create_user = IntegerField(column_name='CreateId', default=0)
    create_time = DateTimeField(column_name='CreateTime', null=True)
    audit_user = IntegerField(column_name='AuditId', default=0)
    audit_time = DateTimeField(column_name='AuditTime', null=True)
    is_wechat = SmallIntegerField(column_name='IsWeiChat', default=1)
    is_deleted = SmallIntegerField(column_name='IsDelete', default=0)
    car_no_count = IntegerField(column_name='EnableCarCount', default=0)

    class Meta:
        table_name = 't_carcard'

class KeTuoCarCardSub(Model):
    card_id = IntegerField(column_name='CardId', default=0)
    provice = CharField(column_name='Province')
    car_no = CharField(column_name='CarplateNum')
    is_card = SmallIntegerField(column_name='IsCard', default=1)
    car_brand = CharField(column_name='CarBrand')
    state = SmallIntegerField(column_name='CarState', default=0)
    remark = CharField(column_name='Remark')
    is_deleted = SmallIntegerField(column_name='IsDelete', default=0)

    class Meta:
        table_name = 't_carcardsub'
    




    


