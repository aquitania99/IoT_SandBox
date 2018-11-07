import os
import sys
import random
import hashlib
import memcache
import redis
import string
import boto
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from time import sleep
import logging
import json

with open('config.json', 'r') as f:
    conf = json.load(f)
aws_id = ''
aws_key = ''

if os.environ['ENV'] in ['local', 'Playground', 'Staging', 'Production']:
    env = os.environ['ENV']
    aws_id = conf[env]['aws']['id']
    aws_key = conf[env]['aws']['key']
else:
    logging.info('BWAHAHAHA!! {0}\n'.format('RUBBISH!'))


logging.basicConfig(filename='lumberjack.out', filemode='a', format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)

### BOTO ACTIONS
# AWS IoT Certificates...
logging.info('*** GET CERTIFICATES FROM S3! ***\n')
conn = boto.connect_s3(aws_id, aws_key)
iot_bucket = conn.get_bucket(os.environ['ENV'].lower()+'-test-iot')
iot_caPath = iot_bucket.get_key('root-CA.crt')  # Root_CA_Certificate_Name
iot_certPath = iot_bucket.get_key(os.environ['ENV']+'Thing-1.cert.pem')  # <Thing_Name>.cert.pem
iot_keyPath = iot_bucket.get_key(os.environ['ENV']+'Thing-1.private.key')  # <Thing_Name>.private.key

iot_caPath.get_contents_to_filename('/tmp/S3-RootCA.crt')
iot_certPath.get_contents_to_filename('/tmp/S3.Pi.cert.pem')
iot_keyPath.get_contents_to_filename('/tmp/S3.Pi.private.key')


def handler(event, context):

    # MEMCACHE
    MEM_SERVER = os.environ['CACHE_HOST']
    MEM_PORT = os.environ['MEM_PORT']
    MEM_TIMEOUT = os.environ['MEM_TIMEOUT']
    # REDIS
    REDIS_SERVER = os.environ['CACHE_HOST']
    REDIS_PORT = os.environ['REDIS_PORT']

    # MemCache Connection
    mem = memcache.Client(['{0}:{1}'.format(MEM_SERVER, MEM_PORT)], debug=0)

    # Redis Connection
    r_db = redis.StrictRedis(host=REDIS_SERVER, port=REDIS_PORT, db=0)

    # TODO implement
    logging.info('GETTING DATA! {0} - {1} - {2}'.format(event,
                                                  event['cardNumber'], event['locationId']))
    try:
        logging.info("Snooze a bit before hitting it!!\n")
        sleep(1)  # Wait a bit before starting....
        letter = list(string.ascii_uppercase)
        pi_number = random.randint(100, 999)

        index = r_db.get('indx-'+event['locationId'])
        logging.info('REDIS STORED LETTER POSITION {0}'.format(index))
        if index is None or int(index) >= 25:
            r_db.set('indx-'+event['locationId'], 0)
            index = int(r_db.get('indx-'+event['locationId']))
        elif int(index) < 26:
            r_db.incr('indx-'+event['locationId'])
            index = int(r_db.get('indx-'+event['locationId']))

        ref_number = letter[index] + str(pi_number)

        logging.info("LETTER + NUMBER: {0}".format(ref_number))

        now = datetime.now().strftime("%Y-%m-%d")
        chain = str(event['cardNumber']) + event['locationId'] + now
        k_hash = hashlib.sha224(chain.encode('utf-8')).hexdigest()
        logging.info("K_HASH: {0}".format(k_hash))

        # MemCache Reference
        # Set Key and Value (hash, ref_number)
        mem.set(k_hash, ref_number, int(MEM_TIMEOUT))

        logging.info("CACHE KEY: {0} - VALUE {1}".format(k_hash, ref_number))
        logging.info("GET MEMCACHE VALUES FOR TEST... {0}".format(mem.get(k_hash)))
        logging.info("Stored to memcached, will auto-expire after 2.5 minutes\n\n")
        logging.info("Request - LocationID {0}\n".format(event['locationId']))
        locations = r_db.keys()
        # logging.info("Redis Locations {0}\n".format(locations))
        if event['locationId'] in str(locations):
            channel = r_db.get(event['locationId']).decode('utf-8')
            logging.info('GETTING CHANNEL FROM REDIS... {0}'.format(channel))
        else:
            channel = 'raspi/test'
            logging.info('DEFAULTING TO CHANNEL... {0}'.format(channel))

        crunch_card = str(event['cardNumber'])

        if mem.get(crunch_card) is None:
            mem.set(crunch_card, 0, 30)
            logging.info("Counter will be incremented on the server by 1, now it's {0}".format(mem.get(crunch_card)))
            publish_number(ref_number, event['locationId'], channel)
            mem.incr(crunch_card, 1)
            logging.info("Counter incremented on the server by 1, now it's {0}".format(mem.get(crunch_card)))
            res = {'status': 'OK', 'refNumber': ref_number}
        elif mem.get(crunch_card) < 6:
            logging.info("Counter will be incremented on the server by 1, now it's {0}".format(mem.get(crunch_card)))
            publish_number(ref_number, event['locationId'], channel)
            mem.incr(crunch_card, 1)
            logging.info("Counter incremented on the server by 1, now it's {0}".format(mem.get(crunch_card)))
            res = {'status': 'OK', 'refNumber': ref_number}
        else:
            mem.delete(crunch_card, 5)
            mem.set(crunch_card, 6, 150)
            res = {'status': 'SLEEPING', 'message': 'Maximum numbers of requests reached.'}
            logging.info(res)

        return res
    except Exception as e:
        logging.info('AND EXCEPTION HAS OCCURRED!! =( {0}'.format(e))


def publish_number(ref_number, location_id, channel):
    sub_channel = channel
    logging.info('CHANNEL: {0}'.format(sub_channel))
    host = os.environ['IOT_HOST']
    root_ca_path = "/tmp/S3-RootCA.crt"
    certificate_path = "/tmp/S3.Pi.cert.pem"
    private_key_path = "/tmp/S3.Pi.private.key"

    try:
        mqtt_client = AWSIoTMQTTClient(os.environ['ENV']+"-Pi")
        mqtt_client.configureEndpoint(host, 8883)
        mqtt_client.configureCredentials(root_ca_path, private_key_path, certificate_path)

        mqtt_client.configureOfflinePublishQueueing(1, dropBehavior=AWSIoTPyMQTT.DROP_OLDEST)
        mqtt_client.configureDrainingFrequency(15)  # Draining: 15 Hz Reqs/Sec
        mqtt_client.configureConnectDisconnectTimeout(6)  # 3 sec
        mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec

        # Connect and subscribe to AWS IoT
        mqtt_client.connect()
        logging.info("Connection Succesful")
        topic = sub_channel
        msg = str(ref_number)

        logging.info("Ref Number Generated... Sending to MQTT")

        logging.info(msg)
        mqtt_client.publish(topic, msg, 0)

        # Disconnect...
        mqtt_client.disconnect()

        return True

    except Exception as e:
        logging.info("Unexpected error: {0}".format(sys.exc_info()[0]))
        logging.info("Exception: {0}".format(e))
