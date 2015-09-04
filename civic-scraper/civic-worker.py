import sys
import uuid
import datetime
from configparser import ConfigParser

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
        self.document_count = 0
        self.load_config()

    def load_config(self):
        '''
        Loads the configuration from disk.
        '''
        try:
            config = ConfigParser()
            config.read('scraper.cfg')
            self.scraper_id = config.get('worker', 'scraper_id')
            self.document_url = config.get('worker', 'document_url')
            self.status_url = config.get('worker', 'status_url')
            self.doc_types = config.get('worker', 'doc_types').split(',')
            logging.info('Scraper: {0}'.format(self.scraper_id))
            logging.info('Documents to: {0}'.format(self.document_url))
            logging.info('Statuses to: {0}'.format(self.status_url))
            logging.info(('Document Types:'
                          ' {0}').format(', '.join(self.doc_types)))

        except:
            logging.error("Unable to load scraper.cfg file.")
            logging.error("The following fields must be included under")
            logging.error("[worker] section within the scraper.cfg file:")
            logging.error("    document_url")
            logging.error("    report_url")
            logging.error("    scraper_id")
            logging.error("Please check the scraper.cfg file, and try again.")
            self.stop()

    def new_doc(self, document):
        '''
        This is a call back from the Worker class.  It uses filters
        to deturmine if the document is of interest, and then sends
        the document to the monther ship.
        '''
        if document['doc_type'] in self.doc_types:
            self.document_count += 1
            payload = {}
            dt_keys = [
                'typed_datetime',
                'creation_datetime',
            ]
            for key in document:
                payload[key] = document[key]
                if key in dt_keys:
                    payload[key] = str(payload[key])
            r = requests.post(self.document_url, data=payload)
            if r.status_code == 200:
                logging.info(('Document Registered: '
                             '{0}').format(document['url']))
            else:
                logging.error(('Document not registered!  '
                              'Error: {0}').format(r.status_code))

    def report_status(self):
        '''
        Reports the workers status to the mothership
        '''
        uptime = datetime.datetime.now() - self.launch_datetime
        status = dict(
           scraper_id=self.scraper_id,
           worker_id=self.worker_id,
           document_count=self.document_count,
           uptime=uptime.total_seconds(),
           bandwidth=self.bandwidth,
        )
        r = requests.post(self.status_url, data=status)
        if r.status_code != 200:
            logging.error('Status not sent! Error: {0}'.format(r.status_code))
        return r.status_code

    def reset(self):
        '''
        Resets all of the information about the worker, and restarts
        the Worker daemon.
        '''
        self.document_count = 0
        restart()


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
