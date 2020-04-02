#!/usr/bin/env python
# -*- coding: utf-8 -*-

#import MySQLdb
#import PySQLPool    # for threads
from database import database
import paho.mqtt.client as paho
import imp
import logging
import signal
import sys
import time
import types
from datetime import datetime
import Queue
import threading
import imp

import sys
reload(sys)
sys.setdefaultencoding('utf8')

try:
    import hashlib
    md = hashlib.md5
except ImportError:
    import md5
    md = md5.new
import os
import socket
from ConfigParser import RawConfigParser, NoOptionError
import codecs
import ast
import re
HAVE_TLS = True
try:
    import ssl
except ImportError:
    HAVE_TLS = False

SCRIPTNAME = 'justsync'
CONFIGFILE = os.getenv(SCRIPTNAME.upper() + 'INI', 'etc/' + SCRIPTNAME + '.ini')
LOGFILE    = os.getenv(SCRIPTNAME.upper() + 'LOG', SCRIPTNAME + '.log')

LWTALIVE   = "1"
LWTDEAD    = "0"

GROUP_NOTIFY_TOPIC = 'sOme_MQTT'

class Config(RawConfigParser):
    
    specials = {
        'TRUE'  : True,
        'FALSE' : False,
        'NONE'  : None,
    }

    def __init__(self, configuration_file):
        RawConfigParser.__init__(self)
        f = codecs.open(configuration_file, 'r', encoding='utf-8')
        self.readfp(f)
        f.close()

        ''' set defaults '''
        self.hostname     = 'localhost'
        self.port         = 1883
        self.username     = None
        self.password     = None
        self.clientid     = SCRIPTNAME
        self.lwt          = 'clients/%s' % SCRIPTNAME
        self.skipretained = False
        self.cleansession = False
        self.protocol = 3

        # db connection
        self.db_host      = 'localhost'
        self.db_port      = 3306
        self.db_user      = None
        self.db_pass      = None
        self.db_name      = 'nnpark'

        self.logformat    = '%(asctime)-15s %(levelname)-5s [%(module)s] %(message)s'
        self.logfile      = LOGFILE
        self.loglevel     = 'DEBUG'

        self.functions    = None
        self.num_workers  = 1

        self.directory    = '.'
        self.ca_certs     = None
        self.tls_version  = None
        self.certfile     = None
        self.keyfile      = None
        self.tls_insecure = False
        self.tls          = False

        self.api_topic    = 'NNPARK'

        self.__dict__.update(self.config('defaults'))

        if HAVE_TLS == False:
            sys.exit(2)

        if self.ca_certs is not None:
            self.tls = True

        if self.tls_version is not None:
            if self.tls_version == 'tlsv1_2':
                self.tls_version = ssl.PROTOCOL_TLSv1_2
            if self.tls_version == 'tlsv1_1':
                self.tls_version = ssl.PROTOCOL_TLSv1_1
            if self.tls_version == 'tlsv1':
                self.tls_version = ssl.PROTOCOL_TLSv1
            if self.tls_version == 'sslv3':
                self.tls_version = ssl.PROTOCOL_SSLv3

        self.loglevelnumber = self.level2number(self.loglevel)

    def level2number(self, level):

        levels = {
            'CRITICAL' : 50,
            'DEBUG' : 10,
            'ERROR' : 40,
            'FATAL' : 50,
            'INFO' : 20,
            'NOTSET' : 0,
            'WARN' : 30,
            'WARNING' : 30,
        }

        return levels.get(level.upper(), levels['DEBUG'])

    def g(self, section, key, default = None):
        try:
            val = self.get(section, key)
            if val.upper() in self.specials:
                return self.specials[val.upper()]
            return ast.literal_eval(val)
        except NoOptionError:
            return default
        except ValueError:
            return val
        except SyntaxError:
            return val
        except:
            return val

    def getlist(self, section, key):

        val = None
        try:
            val = self.get(section, key)
            val = [s.strip() for s in val.split(',')]
        except Exception, e:
            return

        return val

    def getdict(self, section, key):
        val = self.g(section, key)

        try:
            return dict(val)
        except:
            return None

    def config(self, section):

        d = None
        if self.has_section(section):
            d = dict((key, self.g(section, key))
                for (key) in self.options(section) if key not in ['targets', 'module'])
        return d

    def topic_target_list(self, name, topic, data):
        val = None

        try:
            func = load_function(name)
            val = func(topic=topic, data=data, srv=srv)
        except:
            raise

        return val

    def filter(self, name, topic, payload):

        rc = False
        try:
            func = load_function(name)
            rc = func(topic, payload)
        except:
            raise

        return rc

class PeriodicThread(object):

    def __init__(self, callback=None, period=1, name=None, srv=None, now=False, *args, **kwargs):
        self.name          = name
        self.srv           = srv
        self.now           = now
        self.args          = args
        self.kwargs        = kwargs
        self.callback      = callback
        self.period        = period
        self.stop          = False
        self.current_timer = None
        self.schedule_lock = threading.Lock()

    def start(self):

        if self.now == True:
            self.run()

        self.schedule_timer()

    def run(self):

        if self.callback is not None:
            self.callback(srv, *self.args, **self.kwargs)

    def _run(self):

        try:
            self.run()
        except Exception, e:
            logging.exception("Exception in running periodic thread")
        finally:
            with self.schedule_lock:
                if not self.stop:
                    self.schedule_timer()

    def schedule_timer(self):

        self.current_timer = threading.Timer(self.period, self._run)
        if self.name:
            self.current_timer.name = self.name
        self.current_timer.start()

    def cancel(self):

        with self.schedule_lock:
            self.stop = True
            if self.current_timer is not None:
                self.current_timer.cancel()

    def join(self):

        self.current_timer.join()

try:
    cf = Config(CONFIGFILE)
except Exception, e:
    print "Cannot open configuration at %s: %s" % (CONFIGFILE, str(e))
    sys.exit(3)

LOGLEVEL  = cf.loglevelnumber
LOGFILE   = cf.logfile
LOGFORMAT = cf.logformat

if LOGFILE.startswith('stream://'):
    LOGFILE = LOGFILE.replace('stream://', '')
    logging.basicConfig(stream=eval(LOGFILE), level=LOGLEVEL, format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE, level=LOGLEVEL, format=LOGFORMAT)
logging.info("Starting %s" % SCRIPTNAME)
logging.info("Log level is %s" % logging.getLevelName(LOGLEVEL))


mqttc = paho.Client(cf.clientid, clean_session=cf.cleansession, protocol=cf.protocol)

M_QUEUE = {}
exit_flag = False

ptlist = {}
        
class Service(object):
    def __init__(self, mattc, logging, database, cfg):
        
        self.mqttc = mqttc
        
        self.mwcore = globals()

        self.logging = logging

        self.database = database

        self.SCRIPTNAME = SCRIPTNAME

        self.api_topic = 'NNPARK'

        self.group_notify_topic = GROUP_NOTIFY_TOPIC

        self.cfg = cfg

        self.park_handler = None

srv = Service(None, None, None, None)

service_plugins = {}

class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)
    def __repr__(self):
        return '<%s>' % str("\n ".join("%s: %s" % (k, repr(v)) for (k, v) in self.__dict__.iteritems()))
    def get(self, key, default=None):
        if key in self.__dict__ and self.__dict__[key] is not None:
            return self.__dict__[key]
        else:
            return default

    def enum(self):
        item = {}
        for (k, v) in self.__dict__.iteritems():
            item[k] = v
        return item

def get_sections():
    sections = []
    for section in cf.sections():
        if section == 'defaults':
            continue
        if section == 'cron':
            continue
        if section == 'failover':
            continue
        if section.startswith('config:'):
            continue
        if cf.has_option(section, 'targets'):
            sections.append(section)
        else:
            logging.warn("Section `%s' has no targets defined" % section)
    return sections

def get_topic(section):
    if cf.has_option(section, 'topic'):
        return cf.get(section, 'topic')
    return section

def get_qos(section):
    qos = 0
    if cf.has_option(section, 'qos'):
        qos = int(cf.get(section, 'qos'))
    return qos

def get_config(section, name):
    value = None
    if cf.has_option(section, name):
        value = cf.get(section, name)
    return value

def asbool(obj):

    if isinstance(obj, basestring):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError(
                "String is not true/false: %r" % obj)
    return bool(obj)

def parse_cron_options(argstring):

    parts = argstring.split(';')
    options = {'interval': float(parts[0].strip())}
    for part in parts[1:]:
        name, value = part.split('=')
        options[name.strip()] = value.strip()
    return options

def is_filtered(section, topic, payload):
    if cf.has_option(section, 'filter'):
        filterfunc = get_function_name( cf.get(section, 'filter') )
        try:
            return cf.filter(filterfunc, topic, payload)
        except Exception, e:
            pass
    return False

def get_function_name(s):
    func = None

    if s is not None:
        try:
            valid = re.match('^[\w]+\(\)', s)
            if valid is not None:
                func = re.sub('[()]', '', s)
        except:
            pass
    return func

def get_topic_targets(section, topic, data):

    if cf.has_option(section, 'targets'):
        name = get_function_name(cf.get(section, 'targets'))
        try:
            return cf.topic_target_list(name, topic, data)
        except Exception as ex:
            pass
            # to log
    return None

class Job(object):
    def __init__(self, prio, service, section, topic, payload, data, target):
        self.prio    = prio
        self.service = service
        self.section = section
        self.topic   = topic
        self.payload = payload     # raw payload
        self.data    = data        # decoded payload
        self.target = target
        
        logging.info("New `%s:%s' job: %s" % (service, target, topic))
        return
    def __cmp__(self, other):
        return cmp(self.prio, other.prio)

# MQTT broker callbacks
def on_connect(mosq, userdata, flags, result_code):

    if result_code == 0:
        logging.info("Connected to MQTT broker, subscribing to topics...")
        if not cf.cleansession:
            pass

        subscribed = []
        for section in get_sections():
            topic = get_topic(section)
            qos = get_qos(section)

            if topic in subscribed:
                continue

            mqttc.subscribe(str(topic), qos)
            subscribed.append(topic)

        if cf.lwt is not None:
            mqttc.publish(cf.lwt, LWTALIVE, qos=0, retain=True)

    else:
        logging.warning("Connection failed - result code %d" % (result_code))

def on_disconnect(mosq, userdata, result_code):
    print "on_disconnect"

    if result_code == 0:
        logging.info("Clean disconnection from broker")
    else:
        logging.warn("Broker connection lost. Will attempt to reconnect in 5s...")
        #send_failover("brokerdisconnected", "Broker connection lost. Will attempt to reconnect in 5s...")
        time.sleep(5)

def on_message(mosq, userdata, msg):

    topic = msg.topic
    payload = str(msg.payload)

    if topic <> 'PM_MQTT/Local/HeartBeat/':
        logging.info("Message received on %s: %s" % (topic, payload))
    #logging.info("Message received on %s: %s" % (topic, payload))
    #logging.debug("Message received on %s" % topic)

    if msg.retain == 1:
        if cf.skipretained:
            logging.debug("Skipping retained message on %s" % topic)
            return

    for section in get_sections():
        match_topic = get_topic(section)
        if paho.topic_matches_sub(match_topic, topic):
            if is_filtered(section, topic, payload):
                continue
            # Send the message to any targets specified
            send_to_targets(section, topic, payload)

def send_failover(reason, message):
    logging.warn(message)
    send_to_targets('failover', reason, message)

def send_to_targets(section, topic, payload):
    if cf.has_section(section) == False:
        return

    data = None
    
    dispatcher_dict = cf.getdict(section, 'targets')
    function_name = get_function_name(get_config(section, 'targets'))

    if function_name is not None:
        targetlist = get_topic_targets(section, topic, data)
        targetlist_type = type(targetlist)
        if targetlist_type is not types.ListType:
            return
    elif type(dispatcher_dict) == dict:
        def get_key(item):

            modified_topic = item[0].replace('#', chr(0x01)).replace('+', chr(0x02))
            levels = len(item[0].split('/'))

            return "{:03d}{}".format(levels, modified_topic)
        
        sorted_dispatcher = sorted(dispatcher_dict.items(), key=get_key, reverse=True)
        for match_topic, targets in sorted_dispatcher:
            if paho.topic_matches_sub(match_topic, topic):
                targetlist = targets if type(targets) == list else [targets]
                break
        else:
            return
    else:
        targetlist = cf.getlist(section, 'targets')
        if type(targetlist) != list:
            cleanup(0)
            return

    targetlist_resolved = []
    # TODO data
    for target in targetlist:
        try:
            #target = target.format(**data)
            targetlist_resolved.append(target)
        except Exception as ex:
            # log
            pass
    targetlist = targetlist_resolved

    for t in targetlist:
        logging.debug("Message on %s going to %s" % (topic, t))
        service = t
        target = None

        if t.find(':') != -1:
            try:
                service, target = t.split(':', 2)
            except:
                logging.warn("Invalid target %s - should be 'service:target'" % (t))
                continue

        if not service in service_plugins:
            logging.error("Invalid configuration: topic %s points to non-existing service %s" % (topic, service))
            continue
        
        sendtos = None
        if target is None:
            sendtos = get_service_targets(service)
        else:
            sendtos = [target]

        channel = 0
        q_size = len(M_QUEUE)

        try:
            park = GROUP_NOTIFY_TOPIC
            if topic <> GROUP_NOTIFY_TOPIC:
                park = topic.split('/')[2]
            #sub_topic = topic.split('/')[1]

            if park == 'HeartBeat':
                channel = 0 if q_size < 2 else q_size - 2
            elif topic == GROUP_NOTIFY_TOPIC or park == cf.api_topic:
                channel = q_size - 1
            elif park == 'ClientToServer' or park == 'LOCAL': # FIXME: remove this condition
                logging.debug("FIXME!!! topic:%s", topic)
                continue
            else:
                park_id = int(park)
                channel = park_id % (q_size - 2) if q_size > 2 else park_id % q_size
        except Exception, e:
            logging.warn("Invalid channel: topic %s catch exception %s" % (topic, str(e)))
            continue

        for sendto in sendtos:
            job = Job(1, service, section, topic, payload, data, sendto)
            M_QUEUE[channel].put(job)

def get_service_config(service):
    config = cf.config('config:' + service)
    if config is None:
        return {}
    return dict(config)

def get_service_targets(service):
    try:
        targets = cf.getdict('config:' + service, 'targets')
        if type(targets) != dict:
            logging.error("No targets for service `%s'" % service)
            cleanup(0)
    except:
        logging.error("No targets for service `%s'" % service)
        cleanup(0)

    if targets is None:
        return {}
    return dict(targets)

def timeout(func, args=(), kwargs={}, timeout_secs=30, default=False):
    import threading
    class InterruptableThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result = None

        def run(self):
            try:
                self.result = func(*args, **kwargs)
            except Exception, e:
                logging.warn("TIMEOUT function catch exception:%s" % str(e))
                self.result = default

    it = InterruptableThread()
    it.start()
    it.join(timeout_secs)
    if it.isAlive():
        logging.warn("TIMEOUT!!! But thread is alive")
        return default
    else:
        return it.result

def processor(worker_id=None):

    while not exit_flag:
        job = M_QUEUE[worker_id].get()
        
        service = job.service
        section = job.section
        target  = job.target
        topic   = job.topic


        try:
            service_config = get_service_config(service)
            service_targets = get_service_targets(service)
            
            if target not in service_targets:
                error_message = "Invalid configuration"
                raise KeyError(error_message)
        except Exception, e:
            logging.error("Cannot handle service=%s, target=%s: %s" % (service, target, repr(e)))
            M_QUEUE[worker_id].task_done()
            continue

        item = {
            'service' : service,
            'section' : section,
            'target'  : target,
            'config'  : service_config,
            'addrs'   : service_targets[target],
            'topic'   : topic,
            'payload' : job.payload,
            'data'    : None
        }

        if item.get('payload') is not None and len(item.get('payload')) > 0:
            st = Struct(**item)
            notified = False
            try:
                module = service_plugins[service]['module']
                notified = module.plugin(srv, st)
            except Exception, e:
                logging.error("Cannot invoke service for `%s': %s" % (service, str(e)))
            
            if not notified:
                logging.warn("Notification of %s for `%s' FAILED or TIMED OUT" % (service, item.get('topic')))
        else:
            logging.warn("Notification of %s for `%s' suppressed: text is empty" % (service, item.get('topic')))

        M_QUEUE[worker_id].task_done()

    logging.debug("Thread exiting...")

def load_module(path):
    try:
        fp = open(path, 'rb')
        return imp.load_source(md(path).hexdigest(), path, fp)
    finally:
        try:
            fp.close()
        except:
            pass

def load_services(services):
    for service in services:
        service_plugins[service] = {}

        service_config = cf.config('config:' + service)
        if service_config is None:
            logging.error("Service `%s' has no config section" % service)
            sys.exit(1)

        service_plugins[service]['config'] = service_config

        module = cf.g('config:' + service, 'module', service)
        modulefile = 'services/%s.py' % module

        try:
            service_plugins[service]['module'] = load_module(modulefile)
            logging.info("Service %s loaded" % (service))
        except Exception, e:
            logging.error("Can't load %s service (%s): %s" % (service, modulefile, str(e)))
            sys.exit(1)

        #service_plugins[service]['lock'] = threading.Lock()

def connect():

    try:
        services = cf.getlist('defaults', 'launch')
    except:
        logging.error("No services")
        sys.exit(2)

    try:
        os.chdir(cf.directory)
    except Exception, e:
        logging.error("Cannot chdir to %s: %s" % (cf.directory, str(e)))
        sys.exit(2)

    load_services(services)

    park_handler = {}
    for service in service_plugins:
        if service == 'heartbeat':
            continue
        service_config = service_plugins[service]['config']
        if 'parks' not in service_config:
            logging.error("No service parks configured. Aborting")
            sys.exit(3)

        parks = service_config['parks']
        for p in parks:
            park_handler[p] = {}
            park_handler[p]['module'] = service_plugins[service]['module']
            park_handler[p]['info'] = parks[p]
    srv.park_handler = park_handler

    srv.cfg = cf
    srv.api_topic = cf.api_topic
    # db_host   = cf.db_host
    # db_port   = cf.db_port
    # db_user   = cf.db_user
    # db_passwd = cf.db_pass
    # db_name = cf.db_name
    # try:
    #     conn = MySQLdb.connect(host=db_host,
    #                            user=db_user,
    #                            passwd=db_passwd,
    #                            db=db_name,
    #                            charset="utf8")
    # except Exception, e:
    #     logging.error("Cannot connect to mysql: %s" % (str(e)))
    #     sys.exit(2)
    # connection = PySQLPool.getNewConnection(username=cf.db_user,
    #                                         password=cf.db_pass,
    #                                         host=cf.db_host,
    #                                         db=cf.db_name,
    #                                         charset="utf8")
    database.init(cf.db_name,
                  max_connections=25,
                  stale_timeout=60,
                  host=cf.db_host,
                  user=cf.db_user,
                  passwd=cf.db_pass
    )
    srv.database = database

    srv.mqttc = mqttc
    srv.logging = logging

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect

    if cf.username:
        mqttc.username_pw_set(cf.username, cf.password)

    if cf.lwt is not None:
        mqttc.will_set(cf.lwt, payload=LWTDEAD, qos=0, retain=True)

    if cf.tls == True:
        mqttc.tls_set(cf.ca_certs, cf.certfile, cf.keyfile, tls_version=cf.tls_version, ciphers=None)

    if cf.tls_insecure:
        mqttc.tls_insecure_set(True)

    logging.info('Starting %s worker threads' % cf.num_workers)
    for i in range(cf.num_workers):
        M_QUEUE[i] = Queue.Queue(maxsize=0)
        t = threading.Thread(target=processor, kwargs={'worker_id': i})
        t.daemon = True
        t.start()

    try:
        mqttc.connect(cf.hostname, int(cf.port), 60)
    except Exception, e:
        logging.error("Cannot connect to MQTT broker at %s:%d: %s" % (cf.hostname, int(cf.port), str(e)))
        #PySQLPool.terminatePool()
        srv.database.close_all()
        sys.exit(2)

    # logging.info('Starting %s worker threads' % cf.num_workers)
    # for i in range(cf.num_workers):
    #     M_QUEUE[i] = Queue.Queue(maxsize=0)
    #     t = threading.Thread(target=processor, kwargs={'worker_id': i})
    #     t.daemon = True
    #     t.start()

    if cf.has_section('cron'):
        for name, val in cf.items('cron'):
            try:
                func = load_function(name)
                cron_options = parse_cron_options(val)
                interval = cron_options['interval']

                ptlist[name] = PeriodicThread(callback=func, period=interval, name=name, srv=srv, now=asbool(cron_options.get('now')))
                ptlist[name].start()
            except AttributeError:
                logging.error("[cron] section has function [%s] specified, but that's not defined" % name)
                continue

    while not exit_flag:
        reconnect_interval = 5

        try:
            mqttc.loop_forever()
        except socket.error:
            pass
        except:
            # FIXME: add logging with trace
            #raise
            pass

        if not exit_flag:
            logging.warning("MQTT server disconnected, trying to reconnect each %s seconds" % reconnect_interval)
            time.sleep(reconnect_interval)

def load_function(function):
    mod_inst = None

    functions_path = cf.functions
    mod_name,file_ext = os.path.splitext(os.path.split(functions_path)[-1])

    if file_ext.lower() == '.py':
        py_mod = imp.load_source(mod_name, functions_path)

    elif file_ext.lower() == '.pyc':
        py_mod = imp.load_compiled(mod_name, functions_path)

    if hasattr(py_mod, function):
        mod_inst = getattr(py_mod, function)

    return mod_inst

def cleanup(signum=None, frame=None):

    for ptname in ptlist:
        logging.debug("Cancel %s timer" % ptname)
        ptlist[ptname].cancel()

    logging.debug("Disconnecting from MQTT broker...")
    if cf.lwt is not None:
        mqttc.publish(cf.lwt, LWTDEAD, qos=0, retain=True)
    mqttc.loop_stop()
    mqttc.disconnect()

    logging.info("Waiting for queue to drain")
    for k in M_QUEUE.keys():
        M_QUEUE[k].join()

    logging.info("QUEUE Drain DONE")
    # if srv.db is not None:
    #     srv.db.close()
    #PySQLPool.terminatePool()
    srv.database.close_all()

    #logging.info("PySQLPool Terminate DONE")

    global exit_flag
    exit_flag = True

    logging.debug("Exiting on signal %d", signum)
    sys.exit(signum)

if __name__ == '__main__':

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    connect()
