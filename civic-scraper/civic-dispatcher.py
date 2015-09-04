import sys
import uuid
import time

from iddt import Dispatcher
from iddt import Daemon

from threading import Timer

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("civicdocs.civic-scraper")


class CivicDispatcherDaemon(Daemon):

    def __init__(self, pidfile):
        super(CivicDispatcherDaemon, self).__init__(pidfile, stdout='/tmp/stdout.txt')
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
            print(str(e))        

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

    def get_job(self):
        '''
        Get's a job from the mothership.
        '''
        pass

    def dispatch_job(self, job):
        '''
        Dispatches a job to the waiting workers.
        '''
        pass

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
 
