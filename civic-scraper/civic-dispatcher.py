import sys
import uuid
import time
import json
from configparser import ConfigParser
from iddt import Dispatcher
from iddt import Daemon
import requests
from threading import Timer

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("civicdocs.civic-scraper")


class CivicDispatcherDaemon(Daemon):

    def __init__(self, pidfile):
        super(CivicDispatcherDaemon, self).__init__(pidfile)
        self.dispatcher = CivicDispatcher()
        self._running = True

    def run(self):
        '''
        Get's called by the Daemon class once detached from
        the main application instance.  This function sets up the
        status call-in timer as well as sits and trys to grab
        new jobs from the mothership.
        '''

        try:
            self.timer = Timer(60 * 60, self.tick)
            self.timer.start()

            while(self._running):
                job = self.dispatcher.get_job()
                if job is not None:
                    # blocking
                    dispatch_job(job)
                else:
                    # try not to hammer the server *too* bad
                    time.sleep(1)
        except Exception as e:
            logging.error('Error: {0}'.format(str(e)))
            stop()

    def stop(self):
        self._running = False
        super(CivicDispatcherDaemon, self).stop()

    def tick(self):
        '''
        Get's called by the timer when it expires.
        '''
        # restart the timer.
        if self._running:
            self.timer.start()


class CivicDispatcher(Dispatcher):
    '''
    The CivicDispatcher handles dispatching jobs to the running
    workers within the scraper instance.
    '''

    def __init__(self, *args, **kwargs):
        '''
        Class Constructor.
        '''
        super(CivicDispatcher, self).__init__(*args, **kwargs)
        self.dispatch_count = 0
        self.load_config()

    def load_config(self):
        '''
        Loads the configuration from disk.
        '''
        try:
            config = ConfigParser()
            config.read('scraper.cfg')
            self.scraper_id = config.get('dispatcher', 'scraper_id')
            self.jobs_url = config.get('dispatcher', 'jobs_url')
            self.status_url = config.get('dispatcher', 'status_url')
            logging.info('Scraper: {0}'.format(self.scraper_id))
            logging.info('Jobs from: {0}'.format(self.jobs_url))
            logging.info('Statuses to: {0}'.format(self.status_url))

        except:
            logging.error("Unable to load scraper.cfg file.")
            logging.error("The following fields must be included under")
            logging.error("[dispatcher] section within the scraper.cfg file:")
            logging.error("    jobs_url")
            logging.error("    status_url")
            logging.error("    scraper_id")
            logging.error("Please check the scraper.cfg file, and try again.")
            self.stop()

    def get_job(self):
        '''
        Get's a job from the mothership.
        '''
        job = None
        r = requests.get(self.jobs_url)
        if r.status_code is 200:
            job = json.loads(r.text)
        pass

    def dispatch_job(self, job):
        '''
        Dispatches a job to the waiting workers.
        '''
        if job is not None:
            # note: this is blocking until complete
            dispatch(job)

    def report_satus(self, job):
        '''
        Reports the status of the dispatcher to the monthership.
        '''
        pass


if __name__ == '__main__':
    pidfile = '/tmp/worker.pid'
    if len(sys.argv) == 3:
        pidfile = sys.argv[2]
    daemon = CivicDispatcherDaemon(pidfile=pidfile)
    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        elif 'status' == sys.argv[1]:
            daemon.status()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        logger.warning('show cmd deamon usage')
        print("Usage: {} start|stop|restart".format(sys.argv[0]))
        sys.exit(2)
