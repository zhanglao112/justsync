import logging

# log type
LOG_T_DB = "DATABASE"
LOG_T_LOCAL =  "LOCAL"
LOG_T_SERVER =  "SERVER"
LOG_T_INNER  = "INNER"
LOG_FUNCS = {
    'DEBUG': logging.debug,
    'WARN': logging.warn,
    'INFO': logging.info,
    'ERROR': logging.error
}

def do_log(srv, item, log_type, message_type, message):
    if log_type not in LOG_FUNCS.keys():
        return

    logging_func = LOG_FUNCS[log_type]
    logging_func("SERVICE=[%s], TYPE=[%s], MESSAGE=%s",
                 item.service, message_type, message)
