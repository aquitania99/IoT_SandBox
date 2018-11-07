import os
import hashlib
import memcache
from datetime import datetime


def handler(event, context):
    # MEMCACHE PARAMETERS
    MEM_SERVER = os.environ['ENVIRO_CACHE_HOST']
    MEM_PORT = os.environ['ENVIRO_MEM_PORT']

    # MemCache Connection
    mem = memcache.Client(['{0}:{1}'.format(MEM_SERVER, MEM_PORT)])

    # TODO implement
    print('GETTING STUFF! {0} - {1} - {2} - {3}'.format(event, event['cardNumber'], event['locationId'], event['refNumber']))
    try:
        pi_number = event['refNumber']
        now = datetime.now().strftime("%Y-%m-%d")
        chain = str(event['cardNumber']) + event['locationId'] + now
        k_hash = hashlib.sha224(chain.encode('utf-8')).hexdigest()
        print("K_HASH: {0}".format(k_hash))
        print("RefNumber: {0}".format(pi_number))

        # MemCache Reference
        print("CACHE KEY: {0} - VALUE {1}".format(k_hash, pi_number))
        print("GET MEMCACHE VALUES FOR TEST... {0}".format(mem.get(k_hash)))

        if mem.get(k_hash) == pi_number:
            res = {'status': 'OK', 'refNumber': pi_number}
        else:
            res = {'status': 'FAIL', 'message': 'invalid parameters'}
        return res
    except Exception as e:
        print('AND EXCEPTION HAS OCCURRED!! =( {0}'.format(e))
