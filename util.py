#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import datetime
from Crypto.Cipher import AES  
from binascii import b2a_hex, a2b_hex

class Prpcrypt(object):
    def __init__(self, key):  
        self.key = key  
        self.mode = AES.MODE_CBC  
       
    def encrypt(self, text):  
        cryptor = AES.new(self.key, self.mode, self.key)  
        length = 16  
        count = len(text)  
        if(count % length != 0) :  
            add = length - (count % length)  
        else:  
            add = 0

        text = text + ('\0' * add)  
        self.ciphertext = cryptor.encrypt(text)  
        return b2a_hex(self.ciphertext)  
       

    def decrypt(self, text):  
        cryptor = AES.new(self.key, self.mode, self.key)  
        plain_text = cryptor.decrypt(a2b_hex(text))  
        return plain_text.rstrip('\0') 


def str2datetime(datetime_str):
    
    marked = datetime_str.find('.')
    if marked <> -1:
        datetime_str = datetime_str[0: marked]

    return datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

### [minute]
### [hour, minute]
### [day, hour, minute]
def time2minute(stay_time):

    minute = 0
    times_with_blank = []
    times_list = []

    stay_time = stay_time.strip()
    times_with_blank = re.split(u'天|小时|分钟| ', stay_time)

    for t in times_with_blank:
        if t != '':
            times_list.append(str(t))

    day_number    = 0
    hour_number   = 0
    minute_number = 0
    if len(times_list) == 0 or len(times_list) > 3:
        return 0
    elif len(times_list) == 1:
        try:
            number = int(times_list[0])
        except:
            return 0

        if stay_time.find('天') <> -1:
            minute_number = number * 24 * 60
            return minute_number
        elif stay_time.find('小时') <> -1:
            minute_number = number * 60
            return minute_number
        elif stay_time.find('分钟') <> -1:
            minute_number = number
            return minute_number
        minute_number = number

        return minute_number
    elif len(times_list) == 2:
        try:
            hour_number = int(times_list[0])
            minute_number = int(times_list[1])
        except:
            return 0

        minute = hour_number * 60 + minute_number
        return minute
    elif len(times_list) == 3:
        try:
            day_number = int(times_list[0])
            hour_number = int(times_list[1])
            minute_number = int(times_list[2])
        except:
            return 0

        minute = day_number * 24 * 60 + hour_number * 60 + minute_number
        return minute
    else:
        return 0

