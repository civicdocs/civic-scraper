import sys
import uuid
import datetime
import json
from configparser import ConfigParser
from threading import Timer

from iddt import Worker
import requests

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("civicdocs.civic-scraper")


class CivicWorker(Worker):
    '''
    CivicWorker class inherits from iddt.Worker.  This class performs
    all of the web scraping and document discovery.  This class also
    sends documents to the monthership as they are found.
    '''

    def __init__(self, *args, **kwargs):
        '''
        Class Constructor.
        '''
        super(CivicWorker, self).__init__(*args, **kwargs)

        self.launch_datetime = datetime.datetime.now()
        self.worker_id = '{0}-{1}'.format(str(uuid.uuid4()), str(uuid.uuid4()))
        self.tick_rate = 1 #args['tick_rate']
        self.reset()
        self.load_config()
        self.announce()

    def reset(self):
        '''
        Resets the items within the class that change over time.
        '''
        self.document_count = 0

    def load_config(self):
        '''
        Loads the configuration from disk.
        '''
        try:
            config = ConfigParser()
            config.read('scraper.cfg')
            self.dispatcher_id = config.get('dispatcher', 'dispatcher_id')
            self.token = config.get('global', 'token')
            self.announce_url = config.get('worker', 'announce_url')
            self.document_url = config.get('worker', 'document_url')
            self.status_url = config.get('worker', 'status_url')
            self.doc_types = config.get('worker', 'doc_types').split(',')
            #logging.info('Token: {0}'.format(self.scraper_id))
            logging.info('Announce: {0}'.format(self.announce_url))
            logging.info('Documents: {0}'.format(self.document_url))
            logging.info('Status: {0}'.format(self.status_url))
            logging.info(('Document Types:'
                          ' {0}').format(', '.join(self.doc_types)))

        except:
            logging.error("Unable to load scraper.cfg file.")
            logging.error("The following fields must be included under")
            logging.error("[dispatcher] section within the scraper.cfg file:")
            logging.error("    dispatcher_id")
            logging.error("The following fields must be included under")
            logging.error("[worker] section within the scraper.cfg file:")
            logging.error("    announce_url")
            logging.error("    document_url")
            logging.error("    report_url")
            logging.error("The following fields must be included under the")
            logging.error("[global] secont within the scraper.cfg file:")
            logging.error("    token")
            logging.error("Please check the scraper.cfg file, and try again.")
            self.stop()

    def run(self):
        #self.timer = Timer(self.tick_rate, self.tick)
        #self.timer.start()
        super(CivicWorker, self).run()

    def tick(self):
        '''
        Get's called by the timer when it expires
        '''
        self.report_status()
        self.timer = Timer(self.tick_rate, self.tick)
        self.timer.start()

    def announce(self):
        '''
        Announces the worker's presense to the mothership, thus creating
        an entry for it in the database.
        '''
        worker = None
        success = False
        try:
            announce_url = '{0}?token={1}'.format(
                self.announce_url, self.token,
            )
            payload = dict(
                dispatcher_id=self.dispatcher_id,
            )
            r = requests.post(announce_url, data=json.dumps(payload))
            if r.status_code == 200:
                print("\r\n"); print(r.text); print("\r\n");
                worker = json.loads(r.text)['worker']
                self.worker_id = worker['id']
            else:
                logging.error('Announce not sent! Error: {0}'.format(r.status_code))
                success = False
            success = True
        except Exception as e:
            logging.error('Announce not sent! Error: {0}'.format(e))
        return success

    def new_doc(self, document):
        '''
        This is a call back from the Worker class.  It uses filters
        to deturmine if the document is of interest, and then sends
        the document to the monther ship.
        '''
        logging.info("New Document Found!: {0} - {1}".format(document['doc_type'], document['url']))
        if document['doc_type'] in self.doc_types:
            self.document_count += 1
            payload = {}
            dt_keys = [
                'typed_datetime',
                'creation_datetime',
            ]
            del document['_id'] # remove mongo _id key,val
            for key in document:
                payload[key] = document[key]
                if key in dt_keys:
                    payload[key] = str(payload[key])
            
            print("\r\n"); print(payload); print("\r\n")
            try:
                document_url = '{0}?token={1}'.format(
                    self.document_url, self.token,
                )
                r = requests.post(document_url, data=json.dumps(payload))
                if r.status_code == 200:
                    logging.info(('Document Registered: '
                                  '{0}').format(document['url']))
                else:
                    logging.error(('Document not registered!  '
                                   'Error: {0}').format(r.status_code))
            except Exception as e:
                 logging.error(('Document not registered!  '
                              'Error: {0}').format(str(e)))

    def report_status(self):
        '''
        Reports the workers status to the mothership
        '''
        up_time = datetime.datetime.now() - self.launch_datetime
        status = dict(
           #token=self.token,
           worker_id=self.worker_id,
           document_count=self.document_count,
           up_time=up_time.total_seconds(),
           bandwidth=self.bandwidth,
        )
        success = False
        r = requests.post(self.status_url, data=status)
        if r.status_code != 200:
            logging.error('Status not sent! Error: {0}'.format(r.status_code))
        return r.status_code

#worker = CivicWorker(pidfile='/tmp/civic-worker.pid')
#worker.register_callback(worker.new_doc)
#worker.run()

if __name__ == '__main__':
    pidfile = '/tmp/worker.pid'
    if len(sys.argv) == 3:
        pidfile = sys.argv[2]
    worker = CivicWorker(pidfile=pidfile)
    worker.register_callback(worker.new_doc)
    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            worker.start()
        elif 'stop' == sys.argv[1]:
            worker.stop()
        elif 'restart' == sys.argv[1]:
            worker.restart()
        elif 'status' == sys.argv[1]:
            worker.status()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("Usage: {} start|stop|restart".format(sys.argv[0]))
        sys.exit(2)
