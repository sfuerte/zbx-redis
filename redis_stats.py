#!/usr/bin/env /usr/bin/python
'''Redis module to query Redis Server and get
results that can then be used by Zabbix.
https://github.com/sfuerte/zbx-redis
'''

import os
import sys
import re
import argparse
import tempfile
import subprocess
import redis


def send_to_zabbix(metrics, zabbix_conf, zabbix_senderhostname):
    rdatafile = tempfile.NamedTemporaryFile(delete=False)

    for i in metrics:
        if re.match(r'^db', i):
            continue

#        if "redis_" in i:
#            key = i.replace("redis_", "redis.")
#        else:
#            key = "redis." +i
        rdatafile.write("%s redis[%s] %s\n" % (zabbix_senderhostname, i, metrics[i]))

    zbx_sender = "zabbix_sender -c " + zabbix_conf +" -i " +rdatafile.name
    if zabbix_senderhostname != '-':
        zbx_sender = zbx_sender + " -s " + zabbix_senderhostname

    rdatafile.close()

    return_code = 0
    return_code = subprocess.call(zbx_sender, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)

    os.remove(rdatafile.name)

    return return_code


def main():

    parser = argparse.ArgumentParser(description='Zabbix Redis status script')
    parser.add_argument('redis_hostname', nargs='?',
                        help='Redis server address or name (default: 127.0.0.1)',
                        default='127.0.0.1')
    parser.add_argument('metric', nargs='?', help='Which metric to evaluate', default=None)
    parser.add_argument('db', nargs='?', default=None)
    parser.add_argument('-p', '--port', dest='redis_port', action='store',
                        help='Redis server port (default: 6379)', default=6379, type=int)
    parser.add_argument('-a', '--reds_pass', dest='redis_pass', action='store',
                        help='Redis server password (default none)', default=None)
    parser.add_argument('-z', '--zabbix_server', dest='zabbix_server', action='store',
                        help='Zabbix server address/hostname', default='127.0.0.1')
    parser.add_argument('--single-metric', dest='metric_single',
                        help='Check for the single metric only and do NOT send any data to Zabbix Trapper (default: false)',
                        default=False, action='store_true')
    parser.add_argument('--conf', dest='zabbix_conf', action='store',
                        help='Location of Zabbix Agent config (default: /etc/zabbix/zabbix_agentd.conf)',
                        default='/etc/zabbix/zabbix_agentd.conf')
    parser.add_argument('--senderhostname', dest='zabbix_senderhostname', action='store',
                        help='Allows including a sender parameter on calls to zabbix_sender (default empty)',
                        default='-')
    args = parser.parse_args()

    try:
        redis_conn = redis.StrictRedis(host=args.redis_hostname, port=args.redis_port, password=args.redis_pass)
        redis_conn.ping()
    except Exception as ex:
        print 'Fatal Error:', ex
        sys.exit(1)

    server_info = redis_conn.info()

    if args.metric == 'ping' and redis_conn.ping():
        print 'PONG'

    elif args.metric:

        if args.db and args.db in server_info.keys():
            server_info['key_space_db_keys'] = server_info[args.db]['keys']
            server_info['key_space_db_expires'] = server_info[args.db]['expires']
            server_info['key_space_db_avg_ttl'] = server_info[args.db]['avg_ttl']

        def llen():
            print redis_conn.llen(args.db)

        def llensum():
            llensum = 0
            for key in redis_conn.scan_iter('*'):
                if redis_conn.type(key) == 'list':
                    llensum += redis_conn.llen(key)
            print llensum

        def list_key_space_db():
            if args.db in server_info:
                print args.db
            else:
                print 'database_detect'

        def default():
            if args.metric in server_info.keys():
                print server_info[args.metric]

        {
            'llen': llen,
            'llenall': llensum,
            'list_key_space_db': list_key_space_db,
        }.get(args.metric, default)()

    if not args.metric_single:
#        a = []
#        for i in server_info:
#            a.append(Metric(args.redis_hostname, ('redis[%s]' % i), server_info[i]))
#
        llensum = 0
        for key in redis_conn.scan_iter('*'):
            if redis_conn.type(key) == 'list':
                llensum += redis_conn.llen(key)
        server_info['llensum'] = llensum
#        a.append(Metric(args.redis_hostname, 'redis[llenall]', llensum))

        # Send packet to zabbix
        send_to_zabbix(server_info, args.zabbix_conf, args.zabbix_senderhostname)

if __name__ == '__main__':
    main()
