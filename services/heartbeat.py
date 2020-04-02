#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import datetime
from services.plugin import BaseService, CommonService
from database import handle_park_heartbeat
from log import LOG_T_DB, LOG_T_LOCAL, LOG_T_SERVER, LOG_T_INNER
from log import do_log

SYM_HEART_BEAT_TYPE = 'sym_node_host'

def _heartbeat_cb(srv, item, payload):
    try:
        data    = payload[SYM_HEART_BEAT_TYPE]
        fields  = data['data']
        node_id = fields['node_id']
        park_id = int(node_id[node_id.rfind('_') + 1 :])
    except Exception, e:
        message = "[HEARTBEAT_CB] payload [%s] catch exception [%s]" % (str(payload), str(e))
        do_log(srv, item, "WARN", LOG_T_LOCAL, message)
        return False

    last_heartbeat_time = datetime.datetime.now()
    notified = handle_park_heartbeat(srv, item, park_id, None, last_heartbeat_time)
    
    return notified

def _ali_mqtt_heartbeat_cb(srv, item, payload):
    client_id = payload['clientId']
    event_type = payload['eventType']

    if not item.addrs.has_key(client_id):
        return True

    park_id = item.addrs[client_id]
    status = 0 if event_type == 'tcpclean' else 1
    notified = handle_park_heartbeat(srv, item, park_id, status, None)

    return notified

class SymHeartBeatService(CommonService):
    def __init__(self, srv, item):
        super(SymHeartBeatService, self).__init__(srv, item)

class AliMqttHeartBeat(BaseService):
    def __init__(self, srv, item):
        super(AliMqttHeartBeat, self).__init__(srv, item)

    def handle_client(self):
        _srv = self.srv
        _item = self.item
        payload = item.get('payload')

        try:
            payload = json.loads(payload)
        except Exception, e:
            return False

        notified = _ali_mqtt_heartbeat_cb(_srv, _item, payload)

        return notified

SYMHEARTBEAT_CLIENT_CALLBACKS = {
    SYM_HEART_BEAT_TYPE: _heartbeat_cb
}

def plugin(srv, item):
    #srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    notified = True
    if item.get('topic') == srv.group_notify_topic:
        service = AliMqttHeartBeat(srv, item)
        notified = service.handle_client()
    else:
        service = SymHeartBeatService(srv, item)
        service.initialize(True, SYMHEARTBEAT_CLIENT_CALLBACKS)
        notified = service.handle_client()

    if not notified:
        srv.logging.warn("SERVICE=%s, RETURN FALSE.", item.service)

    return notified
