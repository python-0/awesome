#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import time
import logging
import requests
import yaml
import MySQLdb as mysql
import top.api
from raven import Client

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y%m%d %H:%M:%S',
                    filename='/var/log/flushdb.log',
                    filemode='a')

with open('config.yml', 'r') as config:
    try:
        config = yaml.load(config)
    except yaml.YAMLError as ex:
        logging.error(ex)

ENV = os.getenv('ENV', 'staging')
mysql_host = config[ENV]['database']['host']
mysql_port = config[ENV]['database']['port']
mysql_user = config[ENV]['database']['user']
mysql_passwd = config[ENV]['database']['pass']
mysql_db = config[ENV]['database']['dbname']
dayu_key = config[ENV]['dayu']['key']
dayu_secret = config[ENV]['dayu']['secret']
tpl_id = config[ENV]['dayu']['tpl_id']
sentry = Client(config['common']['raven']['url'])

conn = mysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_passwd, db=mysql_db, port=mysql_port)
conn.autocommit(1)
cur = conn.cursor(mysql.cursors.DictCursor)

def send_sms(phone, param):
    req = top.api.AlibabaAliqinFcSmsNumSendRequest()
    req.set_app_info(top.appinfo(dayu_key, dayu_secret))
    req.extend = ""
    req.sms_type = "normal"
    req.sms_free_sign_name = "google"
    req.sms_param = ""
    req.rec_num = phone
    req.sms_template_code = tpl_id
    try :
        resp = req.getResponse(timeout=20)
        logging.info(resp)
    except Exception,e:
        logging.error(e)

def do():
    new_user_list = "select member.mobile_phone mobile, member.id member_id, \
                         member.member_name name, audit.id audit_id, audit.updatetime updatetime \
                     from t_member_rental_audit audit left \
                         JOIN t_member member ON audit.member_id=member.id  \
                     where audit.updatetime BETWEEN (now() - interval 1 HOUR) And (now() - interval 1 SECOND) \
                         and audit.member_status='auditing' limit 10;"
    cur.execute(new_user_list)
    results = cur.fetchall()
    for r in results:
        send_sms(r['mobile'], '')
        logging.info("{}: {} is approved".format(ENV, r))

    now = time.strftime('%Y-%m-%d %H:%M:%S')
    approve_new_user = "update t_member_rental_audit set MEMBER_STATUS='approve',audit_time='{}' where MEMBER_STATUS='auditing';".format(now)
    cur.execute(approve_new_user)


if __name__ == "__main__":
    do()
