import time
import logging
import yaml
from eventzAPI.eventzAPI import Publisher, SubscriberFactory, DS_Logger, DS_Utility, LibrarianClient, DS_Init
from eventzAPI.flatArchiver import  Archiver
import sys
import atexit
from datetime import datetime


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class testps (object):
    def __init__(self, logger, librarianClient, dsParam, userId, subscriber, i):
        self.dsParam = dsParam
        self.subscriber = subscriber
        self.userId = userId
        self.logger = logger
        self.librarianClient = librarianClient
        self.subscriptions = self.dsParam.routingKeys
        self.publications = self.dsParam.publications
        self.filters = []
        self.interTaskQueue = dsParam.interTaskQueue
        self.virtualserver = i

        self.utilities = DS_Utility(self.logger, self.librarianClient, self.dsParam, self.userId)

        # Register code to execute when the application stops.
        # atexit.register(self.stopping)

        # Instantiate a Publisher
        self.publisher = self.dsParam.thePublisher

        # Register code to execute when the application stops.
        atexit.register(self.stopping)




    def run(self):
        t1 = time.time()
        MaxSin = 50000
        #RecsPerSin = 50
        #for n in range(0,RecsPerSin):

        print ("\nSending " + str(MaxSin) + " Test records to Virtual Server VH_" + str(self.virtualserver) +", Starting at 100000000")
        x = 0
        errorcnt = 0
        timeoutcnt = 0

        for i in range(100000001,(100000001+MaxSin)):
            sinNum = str(i)
            partTuple = ("Test Record",sinNum)

            # Instantiate a Publisher
            # publisher = Publisher(dsParam)
            if (x == 160):
                print(errorcnt)
                x = 0
            else:
                print(errorcnt, end='', flush=True)
                x = x+1

            try:
                self.publisher.publish(999000.00, 0, '00000000-0000-0000-0000-000000000000', self.userId, '',
                                               '', sinNum, '', '', partTuple)
            except:
                print ("Failed on Publish " + sinNum )
                errorcnt = errorcnt+1


            timeout = time.time() + 5   # 5 seconds from now
            errormsg = 0

            # while True:
            #     if time.time() > timeout:
            #         errormsg = 1
            #         break
            #     if self.interTaskQueue.empty() == False:
            #         message = self.interTaskQueue.get_nowait()
            #         # LOGGER.info("Message Received: %s", str(message))
            #         self.interTaskQueue.task_done()
            #         recordType = message[0]
            #         # LOGGER.info("Record Type: %s", recordType)
            #         rt = int(float(recordType) * 10)
            #         psTuple = message
            #
            #         if recordType == '999000.00':            # one of my test messages
            #             # LOGGER.info('Received Test back Message: {}'.format(message[0]))
            #             break
            #
            #     else:
            #         time.sleep(.1)      # Wait to prevent the polling from using up cpu resources

            if errormsg == 1:
                print ("\n Timeout!  Message not received back !!!!" + str(timeoutcnt))
                sys.exit()
                if timeoutcnt > 10:
                    break
                timeoutcnt += 1
                x += 1

            time.sleep(.1)


        # Code to execute when the application stops
        # t2 = time.time()
        # t = t2 - t1
        # print("\nTotal time is: " + str(t))
        return (errorcnt)

    def stopping(self):
        print('\nDone Stopping!')
        self.subscriber.stop()
        self.publisher.stop()

    def getmes (self,i):
        errormsg = 0
        timeout = time.time() + 5   # 5 seconds from now
        print ("\nReceiving " + "?" + " Test records to Virtual Server VH_" + str(self.dsParam.virtualhost) +", Starting at 100000000")

        while True:
            if time.time() > timeout:
                errormsg = 1
                break
            if self.interTaskQueue.empty() == False:
                message = self.interTaskQueue.get_nowait()
                # LOGGER.info("Message Received: %s", str(message))
                self.interTaskQueue.task_done()
                recordType = message[0]
                # LOGGER.info("Record Type: %s", recordType)
                rt = int(float(recordType) * 10)
                psTuple = message

                if recordType == '999000.00':            # one of my test messages
                    LOGGER.info('Received Test back Message: {}'.format(message[0]))
                    break

            else:
                time.sleep(.1)      # Wait to prevent the polling from using up cpu resources

        if errormsg == 1:
            print ("\n Timeout!  Message not received back !!!!" )

        return(errormsg)

class APP_Parameters(object):
    def __init__(self,loginDialog, uiPath):
        self.loginDialog = loginDialog
        self.uiPath = uiPath


if __name__ == "__main__":
    ############# start of Processing ###############
    LOGGER.info('Starting I-Tech DS Control.....')

    applicationId = '55555555-5555-5555-5555-55555555555'
    applicationName =  'Python Pub and Sub Test V1.0'

    logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)  # less verbose

    routingKeys = ['999000.00']
    publications = ['999000.00']

    dsInit = DS_Init(applicationId, applicationName)

    dsParam = dsInit.get_params('settings.yaml', routingKeys, publications, None)

    #aPublisher = Publisher(dsParam)
    # aPublisher = dsParam.thePublisher

    fd = dsParam.firstData

    with open ('settings.yaml', 'r') as f:
        appdata = yaml.load(f)
    appParams = APP_Parameters (appdata.get('loginDialog'), appdata.get('uiPath'))

    logger = DS_Logger(dsParam)

    librarianClient = LibrarianClient(dsParam, logger)

    utilities = DS_Utility(logger, librarianClient, dsParam, 'I-Tech')
    userId = 'Jamie'
    # Instantiate a Subscriber Task

    # Instantiate a Subscriber Task
    archiver = Archiver(dsParam.archivePath)
    totalerror = 0
    tstart = datetime.now()

    for i in range(1,5+1):
        dsParam.virtualhost = str('VH_' + str(i))
        dsParam.brokerUserName = str('VH_' + str(i))
        dsParam.brokerPassword = str('VH_' + str(i))
        # Create and start a subscriber thread
        # aSubscriber = SubscriberFactory()
        # subscriber = aSubscriber.makeSubscriber(dsParam, userId, archiver, utilities)
        # subscriber.setName("SubscriberThread")
        # try:
        #     subscriber.start()
        #     # subscriber.run()  # Start watching for messages we are subscribing to
        # except AssertionError as error:
        #     LOGGER.error("Unable to connect to Subscriber: Assertion Error: " + error)
        #     exit()
        # time.sleep(1)
        # Create the Publisher for the application
        aPublisher = Publisher(dsParam)
        # Put it in the parameters
        dsParam.thePublisher = aPublisher
        try:
            testps_ = testps(logger, librarianClient, dsParam, userId, None, i)
        except:
            sys.exit(testps_.stopping())

        currenterror = testps_.run()
        totalerror = totalerror+currenterror

    for x in range(1,5+1):
        dsParam.virtualhost = str('VH_' + str(x))
        dsParam.brokerUserName = str('VH_' + str(x))
        dsParam.brokerPassword = str('VH_' + str(x))
        # Create and start a subscriber thread
        aSubscriber = SubscriberFactory()
        subscriber = aSubscriber.make_subscriber(dsParam, userId, archiver, utilities)
        subscriber.setName("SubscriberThread")
        try:
            subscriber.start()
        except AssertionError as error:
            LOGGER.error("Unable to connect to Subscriber: Assertion Error: " + error)
            exit()
        time.sleep(1)
        testps_.getmes(x)
        # Create the Publisher for the application

    testps_.stopping()

    ttimex = datetime.now()-tstart

    print ("Done all Tests, Error count is " + str(totalerror) + "\nTotal time is " + str(ttimex))



