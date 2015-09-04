# civic-scraper
The civic.io web scraper

##About

The civic-scraper is the workhorse of 
[civicdocs.io](https://github.com/civicdocs/civicdocs.io).  It uses 
[iddt](https://github.com/thequbit/iddt) to find documents ( of specific
types ) on municipality websites, and then report them back to 
civicdocs.io.  The scraper is made up of two parts: the `CivicWorker`
and the `CivicDispatcher`.  The `CivicDispatcher` requests new 
websites to scrape from civicdocs.io, and the `CivicWorker` instances
( you can have any many as your system can handle ) go and scrape 
the websites performing the document discovery.

##Dependancies

There are only dependancies for the civic-scraper: iddt and mongodb.

within a virtualenv ( see: https://virtualenv.pypa.io/en/latest/ )
    $ pip install iddt
    
Then ( for Ubuntu ), install mongodb:
    $ sudo apt-get install mongodb

##Running the civic-scraper

There are two parts of the scraper: the CivicWorker and the CivicDispatcher.  
There should only ever be a single dispatcher running, however you can run 
as many CivicWorkers as you would like.

####CivicWorker

The CivicWorker runs as a daemon, thus it has a specific pid file associated 
with it.  To run a single instance, you can do the following:

    $ python civic-worker.py start

To stop the CivicWorker simply just give it the stop command:

    $ python civic-worker.py stop
    
If you would like to run more than once CivicWorker instance,  you'll need to 
provide a unique pid file name:

    $ python civic-worker.py start /tmp/civic-worker-0.pid
    $ python civic-worker.py start /tmp/civic-worker-1.pid
    
To stop these two workers, you'll need to supply the pid file again:

    $ python civic-worker.py stop /tmp/civic-worker-0.pid
    $ python civic-worker.py stop /tmp/civic-worker-1.pid
    
####CivicDispatcher

You should only every have one CivicDispatcher running at a time.  To run the 
dispatcher, do the following:

    $ python civic-dispatcher.py start
    
The dispatcher will periodicly call into civicdocs.io and check for new jobs to
work on.  As those jobs are available, it will dispatch them to the running CivicWorkers.
Note: if there are no CivicWorkers running, the CivicDispatcher will just sit blocking
forever, so you'll want to run at least (1) CivicWorker for the scraper instance to
work correctly.
