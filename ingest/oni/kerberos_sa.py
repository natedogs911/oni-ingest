#!/bin/env python

import json
import os
import subprocess
import sys
import time

#script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "./etc/master_ingest.json"
worker_conf = json.loads(open (conf_file).read())

def main():
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        while True:
            kb.authenticate()
            print('authenticating')
            time.sleep(18000)

class Kerberos(object):
    _script_path = None
    _conf_file = None
    _kerberos_conf = None
    _kinit = None
    _kinitopts = None
    _keytab = None
    _krb_user = None
    _kinit_args = None

    def __init__(self):

        self._kinit = os.getenv('KINITPATH')
        self._kinitopts = os.getenv('KINITOPTS')
        self._keytab = os.getenv('KEYTABPATH')
        self._krb_user = os.getenv('KRB_USER')

        if self._kinit == None or self._kinitopts == None or self._keytab == None or self._krb_user == None:
            print "Please verify kerberos configuration, some environment variables are missing."
            sys.exit(1)

        self._kinit_args = [self._kinit, self._kinitopts, self._keytab, self._krb_user]

    def authenticate(self):

        kinit = subprocess.Popen(self._kinit_args, stderr=subprocess.PIPE)
        output, error = kinit.communicate()
        if not kinit.returncode == 0:
            if error:
                print error.rstrip()
                sys.exit(kinit.returncode)
        print "Successfully authenticated!"


if __name__ == '__main__':
    main()
