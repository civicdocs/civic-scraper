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
        self.worker_id = None
        self.tick_rate = 60
        self.reset()
        self.load_config()

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
            self.scraper_id = config.get('global', 'scraper_id')
            self.announce_url = config.get('worker', 'announce_url')
            self.document_url = config.get('worker', 'document_url')
            self.status_url = config.get('worker', 'status_url')
            self.doc_types = config.get('worker', 'doc_types').split(',')
            if config.has_option('worker', 'tick_rate'):
                self.tick_rate = int(config.get('worker', 'tick_rate'))
        except:
            logging.error(("Unable to load scraper.cfg file."
                           "The following fields must be included under"
                           "[dispatcher] section within the scraper.cfg file:"
                           "    dispatcher_id"
                           "The following fields must be included under"
                           "[worker] section within the scraper.cfg file:"
                           "    announce_url"
                           "    document_url"
                           "    report_url"
                           "The following fields must be included under the"
                           "[global] secont within the scraper.cfg file:"
                           "    scraper_id"
                           "Check the scraper.cfg file, and try again."))
            self.stop()

    def start(self):
        self.announce()
        super(CivicWorker, self).start()

    def run(self):
        self.timer = Timer(self.tick_rate, self.tick)
        self.timer.start()
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
            up_time = datetime.datetime.now() - self.launch_datetime
            payload = dict(
                scraper_id=self.scraper_id,
                dispatcher_id=self.dispatcher_id,
                document_count=self.document_count,
                up_time=up_time.total_seconds(),
                bandwidth=self.bandwidth,
            )
            r = requests.post(self.announce_url, data=json.dumps(payload))
            if r.status_code == 200:
                worker = json.loads(r.text)['workers']
                self.worker_id = worker['id']
                print('ID: {0}'.format(self.worker_id))
            else:
                logging.error(('Announce not sent! '
                               'Error: {0}').format(r.status_code))
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
        logging.info("New Document Found!: {0} - {1}".format(
            document['doc_type'], document['url'])
        )
        if document['doc_type'] in self.doc_types:
            self.document_count += 1
            payload = dict(
                name='',
                description='',
                url=document['url'],
                source_url=document['source_url'],
                source_url_title=document['source_url_title'],
                link_text=document['link_text'],
                doc_type=document['doc_type'],
            )
            try:
                document_url = self.document_url.replace(
                    '{id}', self.worker_id
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
        payload = dict(
           scraper_id=self.scraper_id,
           dispatcher_id=self.dispatcher_id,
           document_count=self.document_count,
           up_time=up_time.total_seconds(),
           bandwidth=self.bandwidth,
        )
        success = False
        try:
            status_url = self.status_url.replace('{id}', self.worker_id)
            r = requests.put(status_url, data=json.dumps(payload))
            if r.status_code == 200:
                logging.info("Status sent.")
                success = True
            else:
                logging.error(('Status not sent! '
                               'Error: {0}').format(r.status_code))
        except Exception as e:
            logging.error('Status not sent! Error: {0}'.format(str(e)))
        return success


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
