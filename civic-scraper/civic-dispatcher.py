import sys
import uuid
import time
import datetime
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

    def __init__(self, pidfile, tick_rate=60*60):
        super(CivicDispatcherDaemon, self).__init__(pidfile)

        self.dispatcher = CivicDispatcher()
        self._running = True
        self.tick_rate = tick_rate

    def run(self):
        '''
        Get's called by the Daemon class once detached from
        the main application instance.  This function sets up the
        status call-in timer as well as sits and trys to grab
        new jobs from the mothership.
        '''

        try:
            self.timer = Timer(self.tick_rate, self.tick)
            self.timer.start()

            while(self._running):
                success = self.dispatcher.get_job()
                if success:
                    logging.info(
                        "Dispatching URL: {0}".format(
                            self.dispatcher.current_job['url']
                        )
                    )
                    # note: blocking
                    self.dispatcher.dispatch_job()
                    logging.info(
                        "Job Complete for URL: {0}".format(
                            self.dispatcher.current_job['url']
                        )
                    )
                else:
                    # try not to hammer the server *too* bad ...
                    time.sleep(5)
        except Exception as e:
            logging.error('Error: {0}'.format(str(e)))
            self.stop()

    def stop(self):
        self._running = False
        super(CivicDispatcherDaemon, self).stop()

    def tick(self):
        '''
        Get's called by the timer when it expires.
        '''
        if self._running:
            self.dispatcher.report_status()
            self.timer = Timer(self.tick_rate, self.tick)
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
        self.launch_datetime = datetime.datetime.now()
        self.reset()
        self.load_config()
        self.announce()
        self.update_config()

    def reset(self):
        '''
        Resets the items within the class that change over time.
        '''
        self.dispatcher_id = None
        self.dispatch_count = 0
        self.current_job = None

    def load_config(self):
        '''
        Loads the configuration from disk.
        '''
        try:
            config = ConfigParser()
            config.read('scraper.cfg')
            # if we've already run, and announced successfully, we'll have a
            # scraper ID that we can read from the config file.
            if config.has_option('dispatcher', 'dispatcher_id'):
                self.dispatcher_id = config.get('dispatcher', 'dispatcher_id')
            else:
                self.dispatcher_id = None
            self.token = config.get('global', 'token')
            #logging.info('Token: {0}'.format(self.token))
            self.announce_url = config.get('dispatcher', 'announce_url')
            logging.info('Announce: {0}'.format(self.announce_url))
            self.jobs_url = config.get('dispatcher', 'jobs_url')
            logging.info('Jobs from: {0}'.format(self.jobs_url))
            self.status_url = config.get('dispatcher', 'status_url')
            logging.info('Statuses to: {0}'.format(self.status_url))
        except:
            logging.error("Unable to load scraper.cfg file.")
            logging.error("The following fields must be included under the")
            logging.error("[dispatcher] section within the scraper.cfg file:")
            logging.error("    announce_url")
            logging.error("    jobs_url")
            logging.error("    status_url")
            logging.error("The following fields must be included under the")
            logging.error("[global] secont within the scraper.cfg file:")
            logging.error("    token")
            logging.error("Please check the scraper.cfg file, and try again.")
            raise Exception('Incorrect Configuration File.')

    def announce(self):
        '''
        Announces the dispatcher's presense to the mothership, thus creating
        an entry for it in the database.  If we have already announced ourself
        ( if self.dispatcher_id has been populated from scraper.cfg ), then we
        do not need to do this step.
        '''
        if self.dispatcher_id is None:
            print("\r\n\r\n ANNOUNCE \r\n\r\n")
            success = False
            try:
                announce_url = '{0}?token={1}'.format(
                    self.announce_url, self.token,
                )
                r = requests.post(announce_url, data={})
                if r.status_code == 200:
                    dispatcher = json.loads(r.text)['dispatcher']
                    self.dispatcher_id = dispatcher['id']
                else:
                    logging.error('Announce not sent! Error: {0}'.format(r.status_code))
                success = True
            except Exception as e:
                logging.error('Announce not sent! Error: {0}'.format(e))
            return success

    def update_config(self):
        '''
        After we call announce(), we get assigned our scraper_id from the
        mothership.  Once that happens, we need to update our config file 
        on disk so that we don't make a duplicate of ourselves the next
        time we call in, and so the workers running in this scraper
        instance know what dispatcher they belong to.
        '''
        if self.dispatcher_id is None:
            raise Exception(("update_config() must be called after announce() "
                             "has successfully been issued a scraper_id"))
        config = ConfigParser()
        config.read('scraper.cfg') 
        config['dispatcher']['dispatcher_id'] = self.dispatcher_id
        with open('scraper.cfg', 'w') as f:
            config.write(f)

    def get_job(self):
        '''
        Get's a job from the mothership.
        '''
        success = False
        try:
            jobs_url = '{0}?token={1}'.format(
                self.jobs_url, self.token
            )
            r = requests.get(jobs_url)
            if r.status_code is 200:
                job = json.loads(r.text)['job']
                self.current_job = job
                if self.current_job is not None:
                    success = True
            else:
                logging.error('Job not retrieved! Error: {0}'.format(r.status_code))
                success = False
        except Exception as e:
                logging.error('Job not retrieved! Error: {0}'.format(str(e)))
        return success

    def dispatch_job(self):
        '''
        Dispatches a job to the waiting workers.
        '''
        if self.current_job is not None:
            # note: this is blocking until complete
            payload = {
                'target_url': self.current_job['url'],
                'link_level': self.current_job['link_level'],
                'allowed_domains': [],
                'extras': {
                    'job_id': self.current_job['id'],
                },
            }
            self.dispatch(payload)

    def report_status(self):
        '''
        Reports the status of the dispatcher to the monthership.
        '''
        up_time = datetime.datetime.now() - self.launch_datetime
        status = dict(
            #token=self.token,
            dispatcher_id=self.dispatcher_id,
            dispatch_count=self.dispatch_count,
            up_time=up_time.total_seconds(),
            idle=self.idle,
            current_job=self.current_job,
        )
        #print(status)
        success = False
        r = None
        try:
            status_url = '{0}/{1}?token={2}'.format(
                self.status_url, self.dispatcher_id, self.token
            )
            r = requests.post(status_url, data=json.dumps(status))
            if r.status_code == 200:
                success = True
            else:
                logging.error('Status not sent! Error: {0}'.format(r.status_code))
        except Exception as e:
            logging.error('Status not sent! Error: {0}'.format(e))
        return success

#dispatcher = CivicDispatcher()
#dispatcher.get_job()
#time.sleep(1)
#dispatcher.report_status()

dispatcher_daemon = CivicDispatcherDaemon(pidfile='/tmp/civic-dispatcher.pid', tick_rate=10)
dispatcher_daemon.run()

'''
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
'''
