import time
import logging
import yaml
from eventzAPI import SubscriberFactory, DS_Logger, DS_Utility, LibrarianClient, DS_Init
from eventzAPI.flatArchiver import  Archiver
import sys
import atexit


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class testps (object):
    def __init__(self, logger, librarianClient, dsParam, userId, subscriber):
        self.dsParam = dsParam
        self.subscriber = subscriber
        self.userId = userId
        self.logger = logger
        self.librarianClient = librarianClient
        self.subscriptions = self.dsParam.routingKeys
        self.publications = self.dsParam.publications
        self.filters = []
        self.interTaskQueue = dsParam.interTaskQueue

        self.utilities = DS_Utility(self.logger, self.librarianClient, self.dsParam, self.userId)

        # Instantiate a Publisher
        self.publisher = self.dsParam.thePublisher

        # Register code to execute when the application stops.
        atexit.register(self.stopping)




    def run(self):
        t1 = time.time()
        MaxSin = 100

        #RecsPerSin = 50
        #for n in range(0,RecsPerSin):

        print ("Sending " + str(MaxSin) + " Test records, Starting at 100000000")
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
                published = self.publisher.publish(999000.00, partTuple, 0,
                                        '00000000-0000-0000-0000-000000000000',
                                        self.userId, '00000000-0000-0000-0000-000000000000',
                                        False, '00000000-0000-0000-0000-000000000000',
                                        '', '', '', '', '')

                # self.publisher.publish(999000.00, 0, '00000000-0000-0000-0000-000000000000', self.userId, '',
                #                                '', sinNum, '', '', partTuple)
            except:
                print ("Failed on Publish " + sinNum )
                errorcnt = errorcnt+1


            timeout = time.time() + 5   # 5 seconds from now
            errormsg = 0

            while True:
                if time.time() > timeout:
                    errormsg = 1
                    break
                if self.interTaskQueue.empty() == False:
                    message = self.interTaskQueue.get_nowait()
                    LOGGER.info("Message Received: %s", str(message))
                    self.interTaskQueue.task_done()
                    recordType = message[0]
                    LOGGER.info("Record Type: %s", recordType)
                    rt = int(float(recordType) * 10)
                    psTuple = message

                    if recordType == '999000.00':            # one of my test messages
                        LOGGER.info('Received Test back Message: {}'.format(message[0]))
                        break

                # else:
                #     time.sleep(.001)      # Wait to prevent the polling from using up cpu resources

            if errormsg == 1:
                print (" Timeout!  Message not received back !!!!" + str(timeoutcnt))
                if timeoutcnt > 10:
                    break
                timeoutcnt += 1
                x += 1

            # time.sleep(.001)


        # Code to execute when the application stops
        t2 = time.time()
        t = t2 - t1
        print("\nTotal time is: " + str(t))

    def stopping(self):
        print('Stopping!')
        self.subscriber.stop()
        self.publisher.stop()


class APP_Parameters(object):
    def __init__(self,loginDialog, uiPath):
        self.loginDialog = loginDialog
        self.uiPath = uiPath


if __name__ == "__main__":
    ############# start of Processing ###############
    LOGGER.info('Starting I-Tech DS Control.....')

    applicationId = '55555555-5555-5555-5555-55555555555'
    applicationName =  'Python Pub and Sub Test'

    logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)  # less verbose

    routingKeys = ['999000.00']
    publications = ['999000.00']

    dsInit = DS_Init(applicationId, applicationName)

    dsParam = dsInit.get_params('settings.yaml', routingKeys, publications, None)

    #aPublisher = Publisher(dsParam)
    aPublisher = dsParam.thePublisher

    fd = dsParam.firstData

    with open ('../settings.yaml', 'r') as f:
        appdata = yaml.safe_load(f)
    appParams = APP_Parameters (appdata.get('loginDialog'), appdata.get('uiPath'))

    logger = DS_Logger(dsParam)

    librarianClient = LibrarianClient(dsParam, logger)

    utilities = DS_Utility(logger, librarianClient, dsParam, 'I-Tech')
    userId = 'Jamie'
    # Instantiate a Subscriber Task

    # Instantiate a Subscriber Task
    archiver = Archiver(dsParam.archivePath)

    # Create and start a subscriber thread
    aSubscriber = SubscriberFactory()
    subscriber = aSubscriber.make_subscriber(dsParam, userId, archiver, utilities)
#    subscriber.setName("SubscriberThread")
    try:
        subscriber.start()
        # subscriber.run()  # Start watching for messages we are subscribing to
    except AssertionError as error:
        LOGGER.error("Unable to connect to Subscriber: Assertion Error: " + error)
        exit()
    time.sleep(1)
    try:
        testps_ = testps(logger, librarianClient, dsParam, userId, subscriber)
    except:
        sys.exit(testps_.stopping())

    # # Register code to execute when the application stops.
    # atexit.register(testps_.stopping())
    testps_.run()

    print("*** Exiting ***")
    sys.exit(testps_.stopping())