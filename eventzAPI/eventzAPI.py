#
# eventzAPI API
#
# A Programmer's Interface that provides objects needed to implement an eventzAPI Application
#
# COPYRIGHT 2016 - 2017 I-Technology Inc
# all rights reserved
#
# Notes:
# Qt affects the SubscriberThread in that it will be set up as a QThread and will use Qt's signals and slots
# event notification system as opposed to an inter-task queue. When using Qt the interTaskQueue is to be set
# to None.
# When not using Qt, the interTaskQueue needs to be provided.
# e.g.
# from threading import Thread
# from queue import Queue
#
#  q = Queue(maxsize = 0)
#
# Publish and Subscribe parameters are to be passed in as parameters. These parameters can be set in a global file
# e.g. "settings.yaml". The path to the settings file is provided to the ds_init.getParams method during initialization.
# The getParams method returns a DS_Parameters object that can be provided to many of the eventzAPI API methods.
#
# ***** DEPRECATED *******
# Database identifier. This identifier is prepended to DS messages so that when a subscriber receives
# the message it can determine if it is a message that was sent by an application that modifies the local
# database (archive) that its application is using. If so the data will already be in the database and the Subscriber
# does'nt need to put it in the database (archive).  myDBID = uuid.uuid4()
# ************************
# Shared local archives are not allowed. Each local archive will service only one microservice. If the microservice
# subscribes to a record it publishes then the publication will be returned to it through the subscriber task and
# the record will be added to the local archive at that time.
#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
# Version 1.0.0 Jan 1 2017
#   Initial Source for DSAPI
#   Jamie Rodgers
#
# Version 1.0.1
#   Initial Versioning Start
#   June 12, 2018 Jamie Rodgers
#       Added __version__ label
#
# Version 1.0.2
#   Jan 25, 2019 Jamie Rodgers
#       Added Virtual Host parameter 'brokerVirtual' to all Publish/Subscribe
#       was fixed at '/' now can be configured in setting.yaml
#
# Version 1.0.3
#   Jan 29, 2019 Bob Jackson
#       Fixed problems with interTaskQueue for non-Qt applications
#
# Version 1.0.4
#   Jan 31, 2019 Jamie Rodgers
#       removed interTaskQueue as an argument for ds_init
#
# Version 1.0.5
#   Feb 6, 2019 Jamie Rodgers
#       changed Publisher so that is only opens once.
#
# Version 2.0.0 Jamie Rodgers Bob Jackson
#       changed Publisher and Subscriber to remain open.
#
# Version 2.0.1 Bob Jackson
#       changed Gui to SubscriberFactory to create subscribers.
#
# Version 2.0.2 Jamie Rodgers
#       Changed the static MYDBid to allway recreate a new id on start
#       Changed to subcriber queue to be exclusive (now auto delete on server when connection is lost
#
# Version 2.0.3 Jamie Rodgers
#       Changed the publisher.close from closing to stop_consuming so there are no errors on exit
#
#
# Version 2.0.4 Bob Jackson
#       Changed the publish parameters to optional for some parameters.
#       Added RecordAction enum class
#
#
# Version 2.0.5 Bob Jackson
#       Modified parameter lists and parameters to accommodate pika changes in v1.0.0 and v1.0.1
#       pika v 1.0.1 is now required
#       myDBID has been deprecated and will bo longer be generated.
#       Deprecated and added fields to metadata including sessionID to support dedicated messaging within pubsub
#       Modified DS_Parameters and it's arguments - made firstData optional defaulting to 16
#       Modified non-pythonic method names to snake case from camel case
#
# Version 2.0.6 Bob Jackson
#       More snake case conversions
#       added master_archive to DS_Parameters to allow for local archives when there is no master archive
#
# Version 2.0.7 Bob Jackson
#      Up version for PyPi
#
# Version 2.0.8 Bob Jackson
#      Up version for PyPi
#
# Version 2.0.9 Bob Jackson
#      Up version for PyPi
#
# Version 2.0.10 Bob Jackson
#      Fix problem with pub_in in QtSubscriber
#
# Version 2.0.11 Bob Jackson
#       Add ApplicationInitializer class
#       Take archiver out of Subscriber classes
#
# Version 2.0.12 Bob Jackson
#      Up version for PyPi
#
#
# Version 2.0.13 Bob Jackson
#      Added 'parameters' to the return values from ApplicationInitialzer.initialize()
#


import PyQt5.QtCore
from PyQt5.QtCore import pyqtSignal, QThread

__version__ = '2.0.13'

import pika
import uuid
import datetime
from enum import Enum
import ast
import itertools
import operator
from uuid import getnode as get_mac
import yaml
from queue import Queue
import ssl
from threading import Thread
import os
import csv
import json
import time
import threading
import logging
import atexit

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

#
# Parameters object to be instatiated once and passed as a parameter to DSAPI objects and methods
# 19 Parameters
#

class DS_Parameters(object):
    '''
    Parameters object to be instatiated once and passed as a parameter to DSAPI objects and methods
    23 Parameters
    '''

    def __init__(self, exchange, brokerUserName, brokerPassword, brokerIP, sessionID, interTaskQueue, routingKeys,
                 publications, deviceId, deviceName, location, applicationId, applicationName, tenant, archivePath,
                 master_archive, encrypt, pathToCertificate, pathToKey, pathToCaCert, qt, brokerVirtual, thePublisher,
                 firstData = 16):

        # RabbitMQ Parameters
        self.broker_user_name = brokerUserName
        self.broker_password = brokerPassword
        self.broker_IP = brokerIP
        self.virtualhost = brokerVirtual
        self.exchange = exchange

        # self.myDBID = dbID        # Deprecated,

        # Encryption Parameters
        self.path_to_CaCert = pathToCaCert
        self.path_to_certificate = pathToCertificate
        self.path_to_key = pathToKey
        self.encrypt = encrypt

        # Application parameters
        self.session_id = sessionID
        self.archive_path = archivePath
        self.master_archive = master_archive
        self.device_id = deviceId
        self.device_name = deviceName
        self.location = location
        self.application_id = applicationId
        self.application_name = applicationName
        self.tenant = tenant
        self.routing_keys = self.subscriptions = routingKeys # List of Record Types the application subscribes to
        self.publications = publications    # List of record type the application publishes
        self.first_data = firstData           # Offset to data past the metadata
        self.qt = qt                            # True if Qt is the GUI
        self.inter_task_queue = interTaskQueue    # For subscriber messages (non Qt GUIs)
        self.the_publisher = thePublisher        # The one and only publisher object for the application

#
# The DS_Metadata Enum provides an index into the fields of metadata that pre-pend a DS record
#

class DS_MetaData(Enum):
    '''
    The DS_Metadata Enum provides an index into the fields of metadata that pre-pend a DS record
    '''
    recordType = 0
    action = 1
    recordId = 2
    link = 3
    tenant = 4
    userId = 5
    publishDateTime = 6
    applicationId = 7
    versionLink = 8
    versioned = 9
    sessionId = 10          # UUID identifying the user's session so as not to confuse with other user's records
    userMetadata1 = 11      # User defined fields for query filtering
    userMetadata2 = 12
    userMetadata3 = 13
    userMetadata4 = 14
    userMetadata5 = 15

#
# The published record's second field if an action defining the action performed on the record.
#

class RecordAction(Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2
#
# Eventz Application Initializer
# To be instantiated and used to put in place the Eventz objects needed foe a publish & subscribe
# appliction
#

class ApplicationInitializer(object):
    '''
    Eventz Application Initializer
    To be instantiated and used to put in place the Eventz objects needed foe a publish & subscribe
        application:
        publisher: The publisher object handles the publication os event records.
        subscriber: The subscriber task watches for event records in routing_keys and passes them to the app.
        logger: The logger publishes log eventz records (90000002.00)
        LibrarianClient: The librarian client submits queries to the Librarian service and returns the result set.
        utilities: The utlities object has many methods that aid the process.
        parameters: The parameters object containd argument values that are used by the eventzAPI objects.
'''

    def __init__(self, routing_keys, publications, applicationId, applicationName, path_to_settings, user_id):
        self.routing_keys = routing_keys
        self.publications = publications
        self.applicationId = applicationId
        self.applicationName = applicationName
        self.path_to_settings = path_to_settings
        self.user_id = user_id

    pass

    def initialize(self):
        dsInit = DS_Init(self.applicationId, self.applicationName)
        parameters = dsInit.get_params(self.path_to_settings, self.routing_keys, self.publications, None)
        publisher = parameters.the_publisher
        user_id = 'Starting - No user yet.'

        logger = DS_Logger(parameters)
        librarian_client = LibrarianClient(parameters, logger)
        utilities = DS_Utility(logger, librarian_client, parameters, 'I-Tech')
        utilities.start_application(publisher, user_id)

        # Create and start a subscriber thread
        a_subscriber = SubscriberFactory()
        subscriber = a_subscriber.make_subscriber(parameters, self.user_id, utilities)
        subscriber.start()

        return publisher, subscriber, logger, librarian_client, utilities, parameters

#
# The Publisher provides a mechanism for publishing data to the Broker.
# DS_Init will create one and put it in the DS_Parameters object.
#

class Publisher(object):
    '''
    The Publisher provides a mechanism for publishing data to the Broker.
    DS_Init will create one and put it in the DS_Parameters object.
    '''

    def __init__(self,dsParam):
        self.dsParam = dsParam
        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.broker_user_name
        self.brokerPassword = dsParam.broker_password
        self.brokerIP = dsParam.broker_IP
        self.encrypt = dsParam.encrypt
        self.applicationId = dsParam.application_id
        self.tenant = dsParam.tenant
        self.cert = dsParam.path_to_certificate
        self.key = dsParam.path_to_key
        self.CaCert = dsParam.path_to_CaCert
        self.virtualhost = dsParam.virtualhost
        self.channel = None
        self.connection = False
        self.publish_OK = False

        atexit.register(self.stop)

    # The publish method formats a message, adding metadata, and sends it to the AMQP broker
    #
    def publish(self, recordType, dataTuple, action = RecordAction.INSERT.value, link = '00000000-0000-0000-0000-000000000000',
                userId = '', versionLink = '00000000-0000-0000-0000-000000000000', versioned = False,
                sessionID = '00000000-0000-0000-0000-000000000000', umd1 = '', umd2 = '', umd3 = '', umd4 = '',
                umd5 = ''):

        '''
        The publish method formats a message, adding metadata, and sends it to the AMQP broker
        There is to be one publisher keeping one channel open. It is passed through DS_Parameters
        to functions that need to publish

        recordType = 0
        action = 1
        recordId = 2
        link = 3
        tenant = 4
        userId = 5
        publishDateTime = 6
        applicationId = 7
        versionLink = 8
        versioned = 9
        sessionId = 10          # UUID identifying the user's session so as not to confuse with other user's records
        userMetadata1 = 11      # User defined fields for query filtering
        userMetadata2 = 12
        userMetadata3 = 13
        userMetadata4 = 14
        userMetadata5 = 15
        '''

        if self.channel is None or not self.channel.is_open:
            try:
                self.dsParam.the_publisher.reconnect()
            except:
                LOGGER.error('Unable to connect to RabbitMQ server!!!')
                return None

        recordTypeSz = '{:12.2f}'.format(float(recordType)).strip()

        # Prep data here

        recordId = str(uuid.uuid4())        # Give it a new Id
        if link == 0:
            link = '00000000-0000-0000-0000-000000000000'
        if self.tenant == 0:
            self.tenant = '00000000-0000-0000-0000-000000000000'
        if self.applicationId == 0:
            self.applicationId = '00000000-0000-0000-0000-000000000000'

        data = (recordTypeSz, action, recordId, link, self.tenant, userId,
                datetime.datetime.utcnow().isoformat(sep='T'), self.applicationId, versionLink, versioned, sessionID,
                umd1, umd2, umd3, umd4, umd5) + dataTuple

        # Publish it

        rk = "{0:12.2f}".format(float(recordType)).strip()
        self.publish_OK = self.dsParam.the_publisher.channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              body=str(data), properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              )
        if self.publish_OK == None:
            return str(data)
        else:
            return None

    #
    # Publish a tuple that already has the metadata
    #

    def raw_publish(self, recordType, dataTuple):

        '''
        Publish a tuple that already has the metadata (i,e, Metadata doesn't need to be added)
        '''

        data = dataTuple

        rk = "{0:10.2f}".format(recordType).strip()

        if '.00' in rk:
            rk = "{0:10.1f}".format(recordType).strip()

        self.channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              body=str(data))

    def reconnect(self):
        # Set up Broker connection

        if not self.encrypt:
            parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                   port=5672,
                                                   virtual_host=self.virtualhost,
                                                   credentials=(
                                                       pika.PlainCredentials(self.brokerUserName, self.brokerPassword)),
                                                   heartbeat=600,
                                                   ssl_options=None,
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3.0,
                                                   connection_attempts=5)
        else:
            parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                   port = 5671,
                                                   virtual_host = self.vitualhost,
                                                   credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                   heartbeat=600,
                                                   # ssl=True,
                                                   ssl_options = ({
                                                       "ca_certs": self.CaCert,
                                                       "certfile": self.cert,
                                                       "keyfile": self.key,
                                                       "cert_reqs": ssl.CERT_REQUIRED,
                                                       "server_side": False,
                                                       "ssl_version": ssl.PROTOCOL_TLSv1_2
                                                   }),
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3.0,
                                                   connection_attempts=5)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        # result = self.channel.queue_declare(queue=str(self.myQueueName), exclusive=True)
        # queue_name = result.method.queue
        # print ("Publishing Message on Queue: " + queue_name)


    def stop(self):
        LOGGER.info('Publisher Terminating!  Closing Connection')
        if not self.channel is None:
            if self.channel.is_open:
                self.channel.stop_consuming()

#
# The AssuredPublisher provides a mechanism for publishing data to the Broker that is assured (Under development).
#

class AssuredPublisher(object):

    '''
    Not Ready for prime time!
    The AssuredPublisher provides a mechanism for publishing data to the Broker that is assured (Under development).
    '''
    def __init__(self, dsParam):

        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.broker_user_name
        self.brokerPassword = dsParam.broker_password
        self.brokerIP = dsParam.broker_IP
        self.encrypt = dsParam.encrypt
        self.applicationId = dsParam.application_id
        self.tenant = dsParam.tenant
        self.cert = dsParam.path_to_certificate
        self.key = dsParam.path_to_key
        self.CaCert = dsParam.path_to_CaCert
        self.virtualhost = dsParam.virtualhost

        # Set up Broker connection

        if not self.encrypt:
            parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                   port = 5672,
                                                   virtual_host = self.vitualhost,
                                                   credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                   heartbeat_interval=600,
                                                   ssl=False,
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3,
                                                   connection_attempts=5)
        else:
            parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                   port = 5671,
                                                   virtual_host = self.vitualhost,
                                                   credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                   heartbeat_interval=600,
                                                   ssl=True,
                                                   ssl_options = ({
                                                       "ca_certs": self.CaCert,
                                                       "certfile": self.cert,
                                                       "keyfile": self.key,
                                                       "cert_reqs": ssl.CERT_REQUIRED,
                                                       "server_side": False,
                                                       "ssl_version": ssl.PROTOCOL_TLSv1_2
                                                   }),
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3,
                                                   connection_attempts=5)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        self.channel.confirm_delivery(self.delivery_confirmation_callback)
        self.result = self.channel.queue_declare(exclusive=True)
        self.queue_name = self.result.method.queue

    def delivery_confirmation_callback(self, method_frame):

        confirmation_type = method_frame.method.NAME.split('.')[1].lower()

        LOGGER.info('Received %s for delivery tag: %i', confirmation_type, method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)

        LOGGER.info('Published %i messages, %i have yet to be confirmed, %i were acked and %i were nacked',
            self._message_number, len(self._deliveries), self._acked, self._nacked)

    def publish(self, recordType, action, link, userId, applicationId, umd1, umd2, umd3, umd4, umd5,
                dataTuple):


        # Prep data here

        recordId = str(uuid.uuid4())  # Give it a new Id
        if link == 0:
            link = '00000000-0000-0000-0000-000000000000'
        if self.tenant == 0:
            self.tenant = '00000000-0000-0000-0000-000000000000'
        if applicationId == 0:
            applicationId = '00000000-0000-0000-0000-000000000000'

        data = (recordType, action, recordId, link, self.tenant, userId,
                datetime.datetime.utcnow().isoformat(sep='T'), applicationId, umd1, umd2, umd3, umd4,
                umd5) + dataTuple

        # Publish it

        rk = "{0:10.2f}".format(recordType).strip()

        if '.00' in rk:
            rk = "{0:10.1f}".format(recordType).strip()

        self.channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              body=str(data))

    def stop(self):
        LOGGER.info('Publisher Terminating!  Closing Connection')
        if not self.channel is None:
            if self.channel.is_open:
                self.channel.stop_consuming()

#
# Subscriber Thread - A thread watching for messages from a broker
# copyright 2017 I-Technology Inc.
#
# parameter notes:
#     routing_keys is the list of recordTypes that we need to subscribe to
# Usage:
#     from Source.subscriberThread import subscriberThread
#
#     self.subscriberThread = SubscriberThread()    # Create the thread
#     self.subscriberThread.start()  # Start watching for messages we are subscribing to
#     self.subscriberThread.stop()  # Stop thread when application closes
#

class SubscriberFactory(object):

    '''
    Subscriber Thread - A thread watching for messages from a broker
    copyright 2017 I-Technology Inc.

    parameter notes:
        routing_keys is the list of recordTypes that we need to subscribe to
    Usage:
        from Source.subscriberThread import subscriberThread

        self.subscriberThread = SubscriberThread()    # Create the thread
        self.subscriberThread.start()  # Start watching for messages we are subscribing to
        self.subscriberThread.stop()  # Stop thread when application closes
    '''

    def make_subscriber(self, dsParam, applicationUser, utilities):

        if dsParam.qt:
            subscriber = QtSubscriber().SubscriberThread(dsParam, applicationUser, utilities)
        else:
            subscriber = NonQtSubscriber().SubscriberThread(dsParam, applicationUser, utilities)

        return subscriber
'''
    Creates a Subscribing QThread that connects to a Broker and subscribes to record types listed
    in ds_param.routing_keys. Any messages passed by the Broker are passed on through Qt's signals
    and slots facility to the main thread.
'''

class QtSubscriber():

    '''
    A subscriber thread for use with a Gui generated using Qt (PyQt5)
    '''

    # from PyQt5 import QtCore
    # from PyQt5.QtCore import Qt, pyqtSignal, QThread

    # Make a Qt Subscriber Thread

    class SubscriberThread(QThread):
        '''
        Make a QT subscriber thread (QThread)
        Notify parent that a message has been received using QTs signal and slot mechanism
        Process System Messages (9000000.00 - 9100000.99) in this thread
        '''

        # from PyQt5.QtCore import Qt, pyqtSignal, QThread

        pub_in = pyqtSignal(str, str)           # Class variable

        def __init__(self, dsParam, applicationUser, utilities):

            # super().__init__()
            # super(SubscriberThread, self).__init__(self)
            # QThread.__init__(self)
            QThread.__init__(self, None)

            self.exchange = dsParam.exchange
            self.brokerUserName = dsParam.broker_user_name
            self.brokerPassword = dsParam.broker_password
            self.brokerIP = dsParam.broker_IP
            self.encrypt = dsParam.encrypt
            self.applicationId = dsParam.application_id
            self.applicationName = dsParam.application_name
            self.applicationUser = applicationUser
            self.tenant = dsParam.tenant
            self.firstData = dsParam.first_data
            self.archivePath = dsParam.archive_path
            self.master_archive = dsParam.master_archive
            self.deviceId = dsParam.device_id
            self.deviceName = dsParam.device_name
            self.location = dsParam.location
            self.routingKeys = dsParam.routing_keys
            self.systemRoutingKeys = ['9000010.00']
            self.utilities = utilities
            self.cert = dsParam.path_to_certificate
            self.key = dsParam.path_to_key
            self.CaCert = dsParam.path_to_CaCert
            self.virtualhost = dsParam.virtualhost
            self.channel = None
            self.queue_name = None
            self.running = True
            # self.pub_in = self.pub_in

            self.myQueueName = (self.applicationName +  str(uuid.uuid4())).replace(" ", "")

            # Get the Publisher
            self.aPublisher = dsParam.the_publisher
            #
            # workingRoutingKeys = self.routing_keys + self.systemRoutingKeys
            #
            # for routingKey in workingRoutingKeys:
            #     self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=routingKey)
            #
            # LOGGER.info("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)

            atexit.register(self.stop)

        def reconnect(self):
            # Set up RabbitMQ connection

            if not self.encrypt:
                try:
                    parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                           port = 5672,
                                                           virtual_host = self.virtualhost,
                                                           credentials = (pika.PlainCredentials(self.brokerUserName ,
                                                                                                self.brokerPassword)),
                                                           heartbeat=600,
                                                           ssl_options=None,
                                                           socket_timeout=10,
                                                           blocked_connection_timeout=10,
                                                           retry_delay=3,
                                                           connection_attempts=5)
                except Exception as exception:
                    print(type(exception))
                    print(exception)
            else:
                parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                       port = 5671,
                                                       virtual_host = self.virtualhost,
                                                       credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                       heartbeat_interval=600,
                                                       ssl=True,
                                                       ssl_options = ({
                                                           "ca_certs": self.CaCert,
                                                           "certfile": self.cert,
                                                           "keyfile": self.key,
                                                           "cert_reqs": ssl.CERT_REQUIRED,
                                                           "server_side": False,
                                                           "ssl_version": ssl.PROTOCOL_TLSv1_2
                                                       }),
                                                       socket_timeout=10,
                                                       blocked_connection_timeout=10,
                                                       retry_delay=3,
                                                       connection_attempts=5)


            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
            result = self.channel.queue_declare(queue=str(self.myQueueName), exclusive=False )
            self.queue_name = result.method.queue

            workingRoutingKeys = self.routingKeys + self.systemRoutingKeys

            for routingKey in workingRoutingKeys:
                self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=routingKey)

            LOGGER.info("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)
            print("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)


        def run(self):
            if self.running:
                if self.channel is None or not self.channel.is_open:
                    try:
                        self.reconnect()
                    except:
                        LOGGER.error('Unable to connect to RabbitMQ server!!!')

            LOGGER.info("Running Subscriber Thread.")

            # Monitor RMQ and write incoming data to local archive if we have one

            # Create the local archive if none and we want one
            if len(self.archivePath) > 0 and os.path.isfile(self.archivePath) == 0:
                # if os.path.isfile(self.archivePath) == 0:
                LOGGER.info("Creating new local archive")
                file = open(self.archivePath, "a", newline='')
                file.close()

            print("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)
            LOGGER.info("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)

            # Get next request
            self.channel.basic_consume(self.queue_name, on_message_callback=self.callback, auto_ack=True)

            try:
                self.channel.start_consuming()
            except pika.exceptions.StreamLostError:
                LOGGER.info("Stream Lost")
            LOGGER.error("Should never get here !!!")
        #
        # Close connections to RabbitMQ and stop the thread
        #   Launched when thread stopped in pcbMrp when closeEvent() is fired
        #

        def stop(self):
            try:
                LOGGER.info('Subscriber Thread Terminating')
                self.running = False
                if self.channel.is_open:
                    # self.channel.stop_consuming()
                    self.channel.queue_delete(queue=self.queue_name)
                LOGGER.error('Subscriber Thread Terminating1')
                self.connection.close()
                LOGGER.error('Subscriber Thread Terminating2')
            except Exception as e:
                self.connection.process_data_events()
                print(e)

        #
        # Bind the queue to a record type (routing key)
        #
        def bind(self, recordType):

            rk = "{0:10.2f}".format(recordType).strip()

            if '.00' in rk:
                rk = "{0:10.1f}".format(recordType).strip()

            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=rk)

        #
        # Pass the message on to the main application
        #

        def alert_main_app(self, bodyTuple):
        # def alert_main_app(self, bodyTuple):
            '''
             Pass the message on to the main application. If local_archive exists then append the message to it.
            :param bodyTuple:
            :return:
            '''

            # Write the Local Archive if we are subscribing to this record type

            recordType = rt = bodyTuple[0]

            if rt in self.routingKeys or '#' in self.routingKeys:         # Do we care aboutthis message ??

                if len(self.archivePath) > 0 and self.master_archive:     # Write access to Local Archive ??
                    self.utilities.archive(bodyTuple, self.archivePath)     # If so put it in the local archive


                # Alert the main loop

                recordType = bodyTuple[0]
                btz = '{0}'.format(bodyTuple)
                self.pub_in.emit(recordType, btz)
                # self.__class__.pub_in.emit(recordType, btz)
                # self.pubIn.emit(recordType, btz)
                LOGGER.info("Emitted Alert for: ", str(recordType))
                print("Emitted Alert for: ", str(recordType))

                # if self.interTaskQueue != None:
                #     self.interTaskQueue.put(bodyTuple) # Pass it to the main thread
                #     print("Emitted Alert for: ", str(recordType))
                #     print ("Queue: " + self.interTaskQueue)

        #
        # Execute callback when request received
        #

        def callback(self, ch, method, properties, body):

            # print("DS message is ", body.decode('utf_8'))
            LOGGER.info("DS message is ", body.decode('utf_8'))

            bodyStr = body.decode('utf_8')
            i = bodyStr.find(',')  # see who sent this
            sender = bodyStr[2:i - 1]

            # j = bodyStr.find(',', i+1)  # see if this was generated by a Versioner (second field 0 = False, 1 = True)
            # versionSz = bodyStr[i+1:(j)]
            # aVersion = int(versionSz)

            LOGGER.info("External DS message received: %s", bodyStr)
            # bodyStr = bodyStr[i + 1:]  # Make this a tuple without dbid
            # bodyStr = bodyStr[j + 1:]  # Make this a tuple
            # bodyStr = '(' + bodyStr
            bodyTuple = eval(bodyStr)
            recordType = bodyTuple[0]
            tenantNo = bodyTuple[4]
            LOGGER.info("Record Type: %s", str(recordType))
            rt = int(float(recordType) * 100)
            myTime = datetime.datetime.utcnow().isoformat(sep='T')

            LOGGER.info("bodyTuple[firstData]:%s", str(bodyTuple[self.firstData]))
            LOGGER.info("rt: %i", rt)

            #if tenant is not the same then ignore message
            if self.tenant == tenantNo:

                # Handle System Messages

                if int(rt) in range(900000000, 910000099):
                    LOGGER.info("IN RANGE")
                    if float(recordType) == 9000010.00:  # Ping Request
                        LOGGER.info("Got Ping Request")
                        if bodyTuple[self.firstData] == '0':  # All ?
                            LOGGER.info("Ping All Received.")
                            pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                          self.applicationName, myTime)
                            self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                    '00000000-0000-0000-0000-000000000000',
                                                    self.applicationUser, '00000000-0000-0000-0000-000000000000',
                                                    False,  '00000000-0000-0000-0000-000000000000',
                                                    '', '', '', '', '')
                        elif bodyTuple[self.firstData] == '1':  # Device Ping ?
                            deviceWanted = bodyTuple[self.firstData+1].replace('\'', '').strip()
                            LOGGER.info("Device Ping Received:[%s] Me:[%s]", deviceWanted, self.deviceId )
                            if deviceWanted == self.deviceId:
                                LOGGER.info("It's for Me!!")
                                pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                              self.applicationName, myTime)
                                self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                        '00000000-0000-0000-0000-000000000000',
                                                        self.applicationUser,'00000000-0000-0000-0000-000000000000',
                                                        False, '00000000-0000-0000-0000-000000000000',
                                                        '', '', '', '', '')

                        elif bodyTuple[self.firstData] == '2':  # Application Ping
                            appWanted = bodyTuple[self.firstData+2].replace('\'', '').strip()
                            LOGGER.info("Application Ping Received:[%s] Me:[%s]",appWanted,self.applicationId )

                            if appWanted == self.applicationId:
                                LOGGER.info("It's for Me!!")
                                pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                              self.applicationName, myTime)
                            self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                    '00000000-0000-0000-0000-000000000000',
                                                    self.applicationUser, '00000000-0000-0000-0000-000000000000',
                                                    False, '00000000-0000-0000-0000-000000000000',
                                                    '', '', '', '', '')
                        else:
                            LOGGER.info('Invalid Ping Type:' + bodyTuple[self.firstData])

                    # Pass unhandled messages to the main thread

                    else:
                        self.alert_main_app(bodyTuple)

                else:
                    # Here I sent this message
                    self.alert_main_app(bodyTuple)

            # def createSubscriberTask(self):
            #     return self.SubscriberThread(self)
            # Make a normal Subscriber Thread

#
# Creates a Subscribing Thread that connects to a Broker and subscribes to record types listed
# in ds_param.routing_keys. Any messages passed by the Broker are passed on through an inter-task queue
# to the main thread.
#

class NonQtSubscriber():
    '''
        Creates a Subscribing Thread that connects to a Broker and subscribes to record types listed
        in ds_param.routing_keys. Any messages passed by the Broker are passed on through an inter-task queue
        to the main thread. For use with Guis using Tkinter or another non-Qt Gui
    '''

    class SubscriberThread(Thread):

        def __init__(self, dsParam, applicationUser, utilities):

            Thread.__init__(self)
            # super(SubscriberThread, self).__init__()

            self.interTaskQueue = dsParam.inter_task_queue
            self.exchange = dsParam.exchange
            self.brokerUserName = dsParam.broker_user_name
            self.brokerPassword = dsParam.broker_password
            self.brokerIP = dsParam.broker_IP
            self.encrypt = dsParam.encrypt
            self.applicationId = dsParam.application_id
            self.applicationName = dsParam.application_name
            self.applicationUser = applicationUser
            self.tenant = dsParam.tenant
            self.firstData = dsParam.first_data
            self.archivePath = dsParam.archive_path
            self.master_archive = dsParam.master_archive
            self.utilities = utilities
            self.deviceId = dsParam.device_id
            self.deviceName = dsParam.device_name
            self.location = dsParam.location
            self.routingKeys = dsParam.routing_keys
            self.systemRoutingKeys = ['9000010.00']
            # self.archiver = archiver
            self.aPublisher = dsParam.the_publisher
            self.cert = dsParam.path_to_certificate
            self.key = dsParam.path_to_key
            self.CaCert = dsParam.path_to_CaCert
            self.virtualhost = dsParam.virtualhost
            self.channel = None
            self.queue_name = None

            self.myQueueName = (self.applicationName +  str(uuid.uuid4())).replace(" ", "")

            atexit.register(self.stop)

        def reconnect(self):
            # Set up RabbitMQ connection

            if not self.encrypt:
                parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                       port = 5672,
                                                       virtual_host = self.virtualhost,
                                                       credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                       heartbeat=600,
                                                       ssl_options=None,
                                                       socket_timeout=10,
                                                       blocked_connection_timeout=10,
                                                       retry_delay=3.0,
                                                       connection_attempts=5)
            else:
                parameters = pika.ConnectionParameters(host = self.brokerIP,
                                                       port = 5671,
                                                       virtual_host = self.virtualhost,
                                                       credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                                       heartbeat=600,
                                                       # ssl=True,
                                                       ssl_options = ({
                                                           "ca_certs": self.CaCert,
                                                           "certfile": self.cert,
                                                           "keyfile": self.key,
                                                           "cert_reqs": ssl.CERT_REQUIRED,
                                                           "server_side": False,
                                                           "ssl_version": ssl.PROTOCOL_TLSv1_2
                                                       }),
                                                       socket_timeout=10,
                                                       blocked_connection_timeout=10,
                                                       retry_delay=3,
                                                       connection_attempts=5)


            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
            result = self.channel.queue_declare(str(self.myQueueName), exclusive=True )
            self.queue_name = result.method.queue
            workingRoutingKeys = self.routingKeys + self.systemRoutingKeys

            for routingKey in workingRoutingKeys:
                self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=routingKey)

            LOGGER.info("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)
            print("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)

        def run(self):
            if self.channel is None or not self.channel.is_open:
                try:
                    self.reconnect()
                except:
                    LOGGER.error('Unable to connect to RabbitMQ server!!!')
                    return

            LOGGER.info("Running Subscriber Thread.")

            # Monitor RMQ and write incoming data to local archive if we have one

            # Create the local archive if none and we want one
            if len(self.archivePath) > 0 and os.path.isfile(self.archivePath) == 0:
                # if os.path.isfile(self.archivePath) == 0:
                file = open(self.archivePath, "a", newline='')
                file.close()

            print("Waiting for RabbitMQ DS messages from queue: " + self.queue_name)
            LOGGER.info("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)

            # Get next request
            LOGGER.info('queue:' + self.queue_name)
            print('queue:' + self.queue_name)
            self.channel.basic_consume(self.queue_name, on_message_callback=self.callback, auto_ack=True)
            self.channel.start_consuming()
            LOGGER.warning("Subscriber Thread is no longer consuming")

        #
        # Close connections to RabbitMQ and stop the thread
        #   Launched when thread stopped in pcbMrp when closeEvent() is fired
        #

        def stop(self):
            LOGGER.info('Subscriber Thread Terminating')
            if not self.channel is None:
                if self.channel.is_open:
                    try:
                        self.channel.queue_delete(queue=self.queue_name)
                        self.channel.stop_consuming()
                        self.connection.close()
                        print('Terminating Subscriber!')
                    except:
                        self.channel.queue_delete(queue=self.queue_name)
                        self.connection.close()
                        print('Terminating Subscriber! ex')
    #                    self.connection.close()
    #                    self.stop()

        #
        # Bind the queue to a record type (routing key)
        #
        def bind(self, recordType):

            rk = "{0:10.2f}".format(recordType).strip()

            if '.00' in rk:
                rk = "{0:10.1f}".format(recordType).strip()

            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=rk)

        #
        # Pass the message on to the main application
        #

        def alert_main_app(self, bodyTuple):

            # Write the Local Archive if we are subscribing to this record type and there is a local archive

            recordType = rt = bodyTuple[0]

            if (rt in self.routingKeys or '#' in self.routingKeys):     # Do we care about this message ??

                if len(self.archivePath) > 0 and self.master_archive:     # Do we have write access to a local archive ??
                    self.utilities.archive(bodyTuple, self.archivePath)     # If so put it in the local archive

                # Alert the main loop

                if self.interTaskQueue != None:
                    self.interTaskQueue.put(bodyTuple)  # Pass it to the main thread
                    LOGGER.info("Queued Alert for: %s", recordType)
                    # print("Queued Alert for: %s", recordType)
                else:
                    LOGGER.error("No Inter-task data transfer method available to the subscriber task!")

        #
        # Execute callback when request received
        #

        def callback(self, ch, method, properties, body):

            # print("DS message is %s", body.decode('utf_8'))
            LOGGER.info("DS message is %s", body.decode('utf_8'))

            bodyStr = body.decode('utf_8')
            # i = bodyStr.find(',')  # see who sent this
            # sender = bodyStr[2:i - 1]

            # j = bodyStr.find(',', i+1)  # see if this was generated by a Versioner (second field 0 = False, 1 = True)
            # versionSz = bodyStr[i+1:(j)]
            # aVersion = int(versionSz)

            LOGGER.info("External DS message received: %s", bodyStr)
            # bodyStr = bodyStr[i + 1:]  # Make this a tuple without dbid
            # bodyStr = bodyStr[j + 1:]  # Make this a tuple
            # bodyStr = '(' + bodyStr
            bodyTuple = eval(bodyStr)
            recordType = bodyTuple[0]
            tenantNo = bodyTuple[4]
            LOGGER.info("Record Type: %s", str(recordType))
            rt = int(float(recordType) * 100)
            myTime = datetime.datetime.utcnow().isoformat(sep='T')

            LOGGER.info("bodyTuple[firstData]:%s", str(bodyTuple[self.firstData]))
            LOGGER.info("rt: %i", rt)

            #if tenant is not the same then ignore message
            if self.tenant == tenantNo:

                # Handle System Messages

                if int(rt) in range(900000000, 910000099):
                    LOGGER.info("IN RANGE")
                    if float(recordType) == 9000010.00:  # Ping Request
                        LOGGER.info("Got Ping Request")
                        if bodyTuple[self.firstData] == '0':  # All ?
                            LOGGER.info("Ping All Received.")
                            pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                          self.applicationName, myTime)

                            self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                    '00000000-0000-0000-0000-000000000000',
                                                    self.applicationUser, '00000000-0000-0000-0000-000000000000',
                                                    False, '00000000-0000-0000-0000-000000000000',
                                                    '', '', '', '', '')
                        elif bodyTuple[self.firstData] == '1':  # Device Ping ?
                            deviceWanted = bodyTuple[self.firstData + 1].replace('\'', '').strip()
                            LOGGER.info("Device Ping Received:[%s] Me:[%s]", deviceWanted, self.deviceId)
                            if deviceWanted == self.deviceId:
                                LOGGER.info("It's for Me!!")
                                pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                              self.applicationName, myTime)
                                self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                        '00000000-0000-0000-0000-000000000000',
                                                        self.applicationUser, '00000000-0000-0000-0000-000000000000',
                                                        False, '00000000-0000-0000-0000-000000000000',
                                                        '', '', '', '', '')

                        elif bodyTuple[self.firstData] == '2':  # Application Ping
                            appWanted = bodyTuple[self.firstData + 2].replace('\'', '').strip()
                            LOGGER.info("Application Ping Received:[%s] Me:[%s]", appWanted, self.applicationId)

                            if appWanted == self.applicationId:
                                LOGGER.info("It's for Me!!")
                                pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                              self.applicationName, myTime)
                                self.aPublisher.publish(9000011.00, pingRecord, RecordAction.INSERT.value,
                                                        '00000000-0000-0000-0000-000000000000',
                                                        self.applicationUser, '00000000-0000-0000-0000-000000000000',
                                                        False, '00000000-0000-0000-0000-000000000000',
                                                        '', '', '', '', '')
                        else:
                            LOGGER.info('Invalid Ping Type:' + bodyTuple[self.firstData])

                    # Pass unhandled messages to the main thread

                    else:
                        self.alert_main_app(bodyTuple)

                else:
                    self.alert_main_app(bodyTuple)
            else:
                # Here I sent this message
                self.alert_main_app(bodyTuple)

            def createSubscriberTask(self):
                return self.SubscriberThread(self)

    # return SubscriberThread(ds_param, applicationUser, archiver)

#
# Classes for enabling queries of the Archive through the Librarian
#

# class QueryTerm(dict):
#     '''
#     A term in a query consisting of a field name, an operator and the value for the search
#     '''
#     def __init__(self, fieldName, operator, value):
#         # self.fieldName = fieldName
#         # self.operator = operator
#         # self.value = value
#         self = {"fieldName": fieldName,"operator": operator,"value": value}

class  QueryTerm(dict):
    def __init__(self, fieldName, operator, value):
        dict.__init__(self)
        self.update({"fieldName": fieldName,"operator": operator,"value": value})


    # class dsQuery(dict):
#     '''
#     A Query object that will contain one or more QueryTerm s in a list
#     '''
#     def __init__(self, limit, user, tenant, startDate, endDate, queryTerms):
#         # self.limit = limit
#         # self.user = user
#         # self.tenant = str(tenant)
#         # self.startDate = startDate
#         # self.endDate = endDate
#         # self.queryTerms = queryTerms        # A List of QueryTerm
# 	    self = {'limit': limit, 'user': user, 'tenant': tenant, startDate: startDate, 'endDate': endDate, 'queryTerms': queryTerms}

class dsQuery(dict):
	def __init__(self, limit, user, tenant, startDate, endDate, queryTerms):
		dict.__init__(self)
		self.update({'limit': limit, 'user': user, 'tenant': tenant, startDate: startDate, 'endDate': endDate,
                     'queryTerms': queryTerms})



#
# The Librarian Client will send a query to the Librarian through the AMQP broker
#

class LibrarianClient(object):

    '''
    The Librarian Client, when instantiated, accepts dsQuery objects, connects to the Librarian microservice,
    sends the query and awaits a response. When the response arrives it closes the connection and returns the
    response to the caller. Querys are passed using the call method
    e.g. call(userName, tenant, startDate, endDate, limit, queries)
    where:
        userName is an ascii string identifying who is making the query
        tenant is a uuid identifying the tenant in a multi-tenant situation. Only the data belonging to the tenant
            is returned.
        startDate, and endDate when not = '' specify a date range so that only data published within the range is
            returned.
        limit is an integer specifying the maximum number of record to be returned. The result set will be truncated
            to the number specified. A limit ot 0 means all the data will be returned.
        queries is a list of dsQuery objects used by the Librarian service to filter the data. Multiple queries are
            anded. There is no or capability.
    '''

    def __init__(self, dsParam, logger):

        self.brokerIP = dsParam.broker_IP
        self.brokerUserName = dsParam.broker_user_name
        self.brokerPassword = dsParam.broker_password
        self.encrypt = dsParam.encrypt
        self.cert = dsParam.path_to_certificate
        self.key = dsParam.path_to_key
        self.CaCert = dsParam.path_to_CaCert
        self.vitualhost = dsParam.virtualhost

        self.logger = logger

        self.timeout = False

        if not self.encrypt:
            parameters = pika.ConnectionParameters(host=self.brokerIP,
                                                   port=5672,
                                                   virtual_host=self.vitualhost,
                                                   credentials=(
                                                       pika.PlainCredentials(self.brokerUserName, self.brokerPassword)),
                                                   heartbeat=600,
                                                   # ssl=False,
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3,
                                                   connection_attempts=5)
        else:
            parameters = pika.ConnectionParameters(host=self.brokerIP,
                                                   port=5671,
                                                   virtual_host=self.vitualhost,
                                                   credentials=(
                                                       pika.PlainCredentials(self.brokerUserName, self.brokerPassword)),
                                                   heartbeat=600,
                                                   ssl=True,
                                                   ssl_options=({
                                                       "ca_certs"   : self.CaCert,
                                                       "certfile"   : self.cert,
                                                       "keyfile"    : self.key,
                                                       "cert_reqs"  : ssl.CERT_REQUIRED,
                                                       "server_side": False,
                                                       "ssl_version": ssl.PROTOCOL_TLSv1_2
                                                   }),
                                                   socket_timeout=10,
                                                   blocked_connection_timeout=10,
                                                   retry_delay=3,
                                                   connection_attempts=5)

        # credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
        # parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_qos(prefetch_size=0, prefetch_count=10)

        self.channel.basic_consume(self.callback_queue , on_message_callback=self.on_response, auto_ack=True)


    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, userName, tenant, startDate, endDate, limit, queries):

        # Prepare the query(s)
        qdList = []
        t = threading.Timer(60, self.timerExpired) # Query timeout
        for q in queries:
            # qdList.append(q.__dict__)
            qdList.append(q)
        # query = dsQuery(limit, userName, tenant, startDate, endDate, qdList)
        queryDict = {'limit': limit, 'user': userName, 'tenant': tenant, startDate: startDate, 'endDate': endDate,
                'queryTerms': qdList}
        # queryDict = query.__dict__
        querySz = json.dumps(queryDict, cls = to_json)
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',routing_key='rpc_queue',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                   correlation_id=self.corr_id ),body=querySz)
        t.start()  # Start the Timer. If it times out the method timerExpired() will execute
        LOGGER.info("Launching a query %s", qdList)
        while self.response is None and self.timeout==False:
            self.connection.process_data_events()
        if self.timeout == True:
            LOGGER.warning('Librarian not responding')
            print(self, 'Librarian not responding')
            t.cancel()
            self.logger.log('eventzAPI', 0, 0, 0, 'Librarian not responding')
            return None

        # def log(self, user_id, errorType, errorLevel, errorAction, errorText):

        t.cancel()
        return ast.literal_eval(self.response.decode("utf_8"))

    #
    # Timer expiry handler
    #
    def timerExpired(self):
        self.timeout = True


#
# Converts a python object to JSON
#

class to_json(json.JSONEncoder):
    '''
    Converts a python object to JSON
    '''
    def default(self, python_object):
        if isinstance(python_object, time.struct_time):
            return {'__class__': 'time.asctime',
                    '__value__': time.asctime(python_object)}
        if isinstance(python_object, bytes):
            return {'__class__': 'bytes',
                    '__value__': list(python_object)}
        if isinstance(python_object, complex):
            return [python_object.real, python_object.imag]
        return json.JSONEncoder.default(self, python_object)
        # raise TypeError(repr(python_object) + ' is not JSON serializable')

#
# DS_Init allows the loading of parameters from a .yaml file
#

class DS_Init(object):
    '''
    DS_Init loads parameters from a .yaml file into a DS_Parameters object through the getParams method
    e.g. getParams(configurationfile, routing_keys, publications, publisher)
    where:
        configurationfile is a string giving the path to the .yaml file holding some of the parameters
        routing_keys if a list of record types that the application is subscribing to.
        publications is a list of the record types that the allication publishes.
        publisher is the Publisher object instantiated in the calling programme

        additional parameters come from the constructor, namely:

        application_id is a uuid identifier unique to the application and is hard coded by the developer.
        application_name is a string containing a name and version designation and is hard coded by the developer.
        interTaskQueue is a Queue object used by non-Qt guis to pass data from the subscriber task to the main
            application. Initially it defaults to None. The getParams method  will create this queue if the qt parameter
            is False

        The getParams method also creates the Publisher for the application and puts it in the DS_Parmeters object

    '''
    def __init__(self, applicationId, applicationName):
        self.applicationId = applicationId
        self.applicationName = applicationName
        self.interTaskQueue = None

    def get_params(self, configurationfile, routingKeys, publications, publisher):
        LOGGER.info(f"Reading configuration File: {configurationfile}")
        try:
            with open (configurationfile, 'r') as f:
                condata = yaml.safe_load(f)
                brokerExchange = condata['brokerExchange']
                brokerPassword = condata['brokerPassword']
                brokerUserName = condata['brokerUserName']
                brokerIP = condata['brokerIP']
                brokerVirtual = condata['brokerVirtual']
                deviceId = condata['deviceId']
                deviceName = condata['deviceName']
                location = condata['location']
                tenant = condata['tenant']
                localArchivePath = condata['localArchivePath']
                master_archive = condata['master_archive']
                firstData = condata['firstData']
                encrypt = condata['encrypt']
                pathTocacert = condata['pathTocacert']
                pathToCertificate = condata['pathToCertificate']
                pathToKey = condata['pathToKey']
                sessionId = uuid.uuid4()            # Set sessionId here so each instance will have it's own session
                qt = condata['qt']
        except:
            LOGGER.error('Missing settings .yaml File')
            return ''                                   # We need a yaml file. If there isn't one, return empty string

        if deviceId == '':
            deviceId = ':'.join((itertools.starmap(operator.add, zip(*([iter("%012X" % get_mac())] * 2)))))
        if qt:
            self.interTaskQueue = None
        else:
            self.interTaskQueue = Queue()
#        try:
#            if condata('brokerVirtual') == '':
#                condata['brokerVirtual'] == '/'
#        except:
#            condata.insert['brokerVirtual']
#            condata['brokerVirtual'] == '/'

        # with open(configurationfile, 'w') as yaml_file:
        #     yaml_file.write( yaml.dump(condata, default_flow_style=False))

        dsParam = DS_Parameters( brokerExchange, brokerUserName, brokerPassword, brokerIP, sessionId, self.interTaskQueue,
                                 routingKeys, publications, deviceId, deviceName, location, self.applicationId,
                                 self.applicationName, tenant, localArchivePath, master_archive, encrypt,
                                 pathToCertificate, pathToKey, pathTocacert, qt, brokerVirtual, publisher, firstData)

        # Create the Publisher for the application
        aPublisher = Publisher(dsParam)
        # Put it in the parameters
        dsParam.the_publisher = aPublisher

        return (dsParam)


#
# DS_Utility contains helper methods to assist in implementing am I-Tech DataStream application
#

class DS_Utility(object):
    '''
    DS_Utility contains helper methods to assist in implementing an application using this infrastructure
    The methods are:
        refreshArchive - Querys the Librarian for all records and put them in the local archive. Used to initialize the
            local repository on startup or reset
        updateArchive - update either a list or localarchive. To removed all records that have been updated or delected.
            It will also remove and records that are not yours by comparing the tenant records
        archive - A function to append a new DS RECORD to the local Archive.
        match - Find a record in a table where the data in 'column' matches the 'target'. Return the row # or -1 if no
            match found. The table is a Qt QTableWidget
        dayOfYear - Returns the day of the year for a date
        cleanDate - If the OS (Linux) messes up the date format then fix it: Want yyyy-mm-dd from d/m/yy strings
        startApplication - Called when a application starts. Generates and publishes a system message.
        stopApplication - Called when a application stops. Generates and publishes a system message.
    '''
    def __init__(self, logger, librarianClient, dsParam, userId):
        self.logger = logger
        self.librarianClient = librarianClient
        self.dsParam = dsParam
        self.userId = userId        # This is the application user
        self.qt = dsParam.qt

    #
    # Query the Librarian for all records and put them in the local archive.
    #   Used to initialize the local repository on startup or reset
    #

    def refresh_archive(self, loggedInUser, localArchivePath, tenant, subscriptions=None):

        LOGGER.info('Sending query and awaiting a response.')
        if (subscriptions != None and not('#' in subscriptions)):
            with open(localArchivePath, "a", encoding='utf_8', newline='') as localArch:
                # Delete local archive contents
                localArch.seek(0)
                localArch.truncate()
                writer = csv.writer(localArch, delimiter='\t')
                recordCount = 0

                queries = []
                queryTerm = QueryTerm("recordType", "EQ", subscriptions)
                queries.append(queryTerm)
                result = self.librarianClient.call(loggedInUser, tenant, '', '', 0, queries)  # Get all the records
                if result:
                    # Fetch records from the master archive and write them into the local archive
                    for r in result:
                       writer.writerow(r)  # Here at end of record. Write it and clear the string
                       recordCount += 1
            LOGGER.info('Data Transferred from Archive. Wrote %s records.', recordCount)


        else:
            queries = []
            queryTerm = QueryTerm("recordType", "EQ", "*")
            queries.append(queryTerm)
            # query = dsQuery(0, loggedInUser, tenant, '', '', queryTerm)
            result = self.librarianClient.call(loggedInUser, tenant, '', '', 0, queries)  # Get all the records
            LOGGER.info('Result retrieved.')
            if result:
                with open(localArchivePath, "a", encoding='utf_8', newline='') as localArch:

                    # Delete local archive contents
                    localArch.seek(0)
                    localArch.truncate()

                    # Fetch records from the master archive and write them into the local archive
                    writer = csv.writer(localArch, delimiter='\t')
                    recordCount = 0
                    for r in result:
                        writer.writerow(r)  # Here at end of record. Write it and clear the string
                        recordCount += 1

                    LOGGER.info('Data Transferred from Archive. Wrote %s records.', recordCount)


    def update_archive(self, loggedInUser, tenant, localList=None):

        '''
        update_archive(self, loggedInUser, tenant, localList=None)
        update either a list or localarchive. To removed all records that have
        been updated or delected. will also remove records that are not yours by comparing
        the tenant field against the tenant parameter.
        '''

        updateList =[]
        if (localList != None ):
            rdr = localList
        else:
            if self.dsParam.archive_path != '':
                f = open(self.dsParam.archive_path, encoding='utf_8', newline='')
                rdr = csv.reader(f, delimiter="\t")
            else:
                return []    # Empty List if wanting Archive and there is none

        for row in rdr:
            action = row[1]
            recNo = row[2]
            linkNo = row[3]
            tenantNo = row[4]

            if (action == '0' and tenantNo == tenant) :  # Insert New Record
                updateList.append(row)
            elif (tenantNo == tenant):
                found = -1
                index = 0
                for item in updateList:
                    if (item[2] == linkNo and tenantNo == tenant):
                        found = index
                        break
                    index += 1
                if found >= 0:
                    if action == '1':  # Update existing record
                        updateList.pop(index)
                        updateList.append(row)
                    elif action == '2':
                        # Here we are deleting a record
                        updateList.pop(index)
                    else:
                        LOGGER.error('Invalid Record Action. Record action must be 0,1,2')
                else:
                    LOGGER.warning("Updating or Deleting a record not found: ", linkNo)

        if (localList == None):
            f.close()

        return (updateList)

    #
    # A function to append a new Event RECORD to the local Archive.
    #

    def archive(self, record, pathToArchive):

        # if self.lockURL != '':
        #     with SimpleFlock(self.lockURL):
        #         with open (pathToArchive, "a", newline='') as arch:
        #             writer = csv.writer(arch, delimiter='\t')
        #             writer.writerow(record)
        # else:
        #     with open (pathToArchive, "a", newline='') as arch:
        #         writer = csv.writer(arch, delimiter='\t')
        #         writer.writerow(record)

        with open(pathToArchive, "a", newline='') as arch:
            writer = csv.writer(arch, delimiter='\t')
            writer.writerow(record)

        return

    #
    # Find a record in a table where the data in 'column' matches the 'target'. Return the row # or -1 if no match found.
    #   The table is a QT QTableWidget
    #

    def match(self, table, column, target):
        """Find a record in a table where the data in 'column' matches the 'target'. Return the row # or -1 if no match found.
            The table is a QT QTableWidget
            :rtype: int

            This is QT specific

        """
        if self.qt:
            from PyQt5 import QtCore
            from PyQt5.QtCore import Qt, pyqtSignal

            index = 0
            for row in range(0,table.rowCount(), 1):
                item = table.item(row, column)
                #LOGGER.info('Item: %s' %item.data(Qt.DisplayRole))
                if item.data(Qt.DisplayRole) == target:
                    # return table.indexFromItem(item).row()
                    return index
                index += 1
            return -1
        else:
            LOGGER.error('match in DS_Utility is for Qt projects only')

    #
    # Find a record in a list where the data in recordId matches the 'target'. Return the row # or -1 if no match found.
    #

    def matchListItem(aList, target):
        """
        Find row in table that has a value in column that equals target
        :rtype: int
        """
        # LOGGER.info('Rows: %i',aList.rowCount())
        index = 0
        for row in aList:
            item = row[2]
            # LOGGER.info('Item: %s' %item.data(Qt.DisplayRole))
            if item == target:
                # return table.indexFromItem(item).row()
                return index
            index += 1
        return -1

    #
    # Returns the day of the year for a date
    #

    def day_of_year(self, aDate):
        fmt = '%Y-%m-%d'
        dt = datetime.datetime.strptime(aDate, fmt)
        tt = dt.timetuple()
        return tt.tm_yday

    #
    # If the OS (Linux) messes up the date format then fix it:
    # Want yyyy-mm-dd from d/m/yy
    #


    def clean_linux_date(self, aDateString):
        if len(aDateString) == 10:
            return aDateString
        else:
            slash1 = aDateString.find('/', 0)
            slash2 = aDateString.find('/', slash1+1)
            y = aDateString[slash2+1:]
            d = aDateString[slash1+1:slash2]
            if len(d) == 1:
                d = '0' + d
            m = aDateString[0:slash1]
            if len(m) == 1:
                m = '0' + m
            newDateString = '20' + y + '-' + m + '-' + d
            return newDateString

    #
    # Called when a application starts. Generates and publishes a system message.
    #

    def start_application(self, aPublisher, userId):

        newUUID = uuid.uuid4()
        myTime = datetime.datetime.utcnow().isoformat(sep='T')
        pid = os.getpid()

        # g.users.items())[g.currentUser]

        # Restore a local archive if master_archive == True
        if self.dsParam.archive_path != "" and self.dsParam.master_archive:
            self.refresh_archive(userId, self.dsParam.archive_path, self.dsParam.tenant, self.dsParam.routing_keys)

        startRecord = (self.dsParam.device_id, self.dsParam.device_name, self.dsParam.location,
                       self.dsParam.application_id, self.dsParam.application_name, myTime, self.dsParam.subscriptions,
                       self.dsParam.publications, pid)

        aPublisher.publish(9000000.00, startRecord, RecordAction.INSERT.value,
                           '00000000-0000-0000-0000-000000000000',
                           self.userId, '00000000-0000-0000-0000-000000000000',
                           False, '00000000-0000-0000-0000-000000000000',
                           '', '', '', '', '')

    #
    # Called when a application stops. Generates and publishes a system message.
    #

    def stop_application(self, aPublisher):

        newUUID = uuid.uuid4()
        myTime = datetime.datetime.utcnow().isoformat(sep='T')
        pid = os.getpid()

        stopRecord = (self.dsParam.device_id, self.dsParam.device_name, self.dsParam.location,
                       self.dsParam.application_id, self.dsParam.application_name, myTime, pid)

        aPublisher.publish(9000001.00, stopRecord, RecordAction.INSERT.value,
                           '00000000-0000-0000-0000-000000000000',
                           self.userId, '00000000-0000-0000-0000-000000000000',
                           False, '00000000-0000-0000-0000-000000000000',
                           '', '', '', '', '')


#
# A DS_Logger packages an error message in a system message and publishes it. Acts like a regular log message
# but it is subscribed to by a system monitor.
#
# Error Type: Error Code defined by Developer
# Error Level: Error Level for Verbosity filtering
# Error Action code:
# 0 – Display
# 1 – Email Alert Level 1
# 2 – Email Alert Level 2
# 3 – Email Alert Level 3
# 4 – Page Alert
# 5 – Syslog Alert
#

class DS_Logger(object):

    '''
    A DS_Logger packages an error message in a system message and publishes it. Acts like a regular log message
    but it is subscribed to by a system monitor.

    Error Type: Error Code defined by Developer
    Error Level: Error Level for Verbosity filtering
    Error Action code:
    0 – Display
    1 – Email Alert Level 1
    2 – Email Alert Level 2
    3 – Email Alert Level 3
    4 – Page Alert
    5 – Syslog Alert

    '''
    def __init__(self, dsParam):

        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.broker_user_name
        self.brokerPassword = dsParam.broker_password
        self.brokerIP = dsParam.broker_IP
        self.encrypt = dsParam.encrypt
        self.deviceId =  dsParam.device_id
        self.deviceName =  dsParam.device_name
        self.location = dsParam.location
        self.applicationId = dsParam.application_id
        self.applicationName =  dsParam.application_name
        self.tenant = dsParam.tenant


        self.dsParam = dsParam


    #
    # log method when we don't have a Publisher
    #


    def log(self, userID, errorType, errorLevel, errorAction, errorText):

        logPublisher = self.dsParam.the_publisher
        # logPublisher = Publisher(self.exchange, self.brokerUserName, self.brokerPassword, self.brokerIP, self.dbID)

        logTuple = (self.deviceId, self.deviceName, self.location, self.applicationId, self.applicationName, errorType,
                    errorLevel, errorAction, errorText)

        logPublisher.publish(9000020.00, logTuple, RecordAction.INSERT.value,
                             '00000000-0000-0000-0000-000000000000',
                             userID, '00000000-0000-0000-0000-000000000000',
                             False, '00000000-0000-0000-0000-000000000000',
                             '', '', '', '', '')


#
# A State Machine allows an application to recognize state
#
# It uses a state table imported from a tab delimited .csv file generated from a spreadsheet
# The state machine moves from an initail state to other states based on the events it receives
# as arguments to the processEvent() method.
#
# The state table is converted fron .csv format by the SMU class method translateTable()
#
# A transitions dictionary maps the alpha transition method name to a defined method in the application.
# See the tstStateMachine.py example.
#

class StateMachine(object):
    '''
    # A State Machine allows an application to recognize state

    It uses a state table imported from a tab delimited .csv file generated from a spreadsheet
    The state machine moves from an initail state to other states based on the events it receives
    as arguments to the processEvent() method.

    The state table is converted fron .csv format by the SMU class method translateTable()

    A transitions dictionary maps the alpha transition method name to a defined method in the application.
    See the tstStateMachine.py example.

    '''
    # def __init__(self, states, stateTable, eventTable, transitions):
    def __init__(self, states, transitions, stateTable):
        self.states = states
        self.transitions = transitions
        self.stateTable = stateTable
        self.currentState = 0

    #
    # processEvent is called when an event occurs in the main loop or when one is generated by a method
    #

    def process_event(self, event):

        found = False
        for ev in self.stateTable:      # Find the event entry in the state table
            l1 = ev[0]
            l2 = ev[1]
            print('Event{0} {1}'.format(l1[0][1], l1[1][1]))
            if l1[1][1] == event:
                # Here we found the event. Get the action method
                nextState = l1[self.currentState + 2][1]
                action = l2[self.currentState + 2][1]
                print('Action: ' + action)
                found = True
                break
        if found:
            self.currentState = int(nextState)
            fntocall = self.transitions[action]  # look up the transition function
            fntocall()  # go and execute the required transition logic!
        else:
            print("Sorry, event " + str(event) + " was not found. ")


        # action = self.eventTable[event]
        # print(action)

#
# The state table is converted fron .csv format by the SMU class method translateTable()
#

class SMU(object):
    '''
    The state table is converted fron .csv format by the SMU class method translateTable()
    '''
    def __init__(self):
        pass

    #
    # Translate Table From csv
    #
    def translate_table(self, pathToTable):
        rowNo = 0
        states = []
        stateTable = []
        even = True
        with open(pathToTable, "r") as f:
            rdr = csv.reader(f, delimiter="\t")
            for row in rdr:
                # Process a row
                # print('Row #: ' + str(rowNo))
                if rowNo == 0:
                    # Get the States
                    states = list(enumerate(row, start=-2))
                    del states[0:2]

                if rowNo >= 2:
                    # Process an event
                    if even:
                        # event = row[0]
                        # eventDesc = row[1]
                        nextStates = list(enumerate(row, start=-2))
                        even = False
                    else:
                        transitionMethods = list(enumerate(row, start=-2))
                        eventTuple = ( nextStates, transitionMethods)
                        stateTable.append(eventTuple)
                        even = True
                rowNo += 1


        return(states, stateTable)
