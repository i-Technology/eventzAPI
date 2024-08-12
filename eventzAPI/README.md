# eventzAPI
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v1.4%20adopted-ff69b4.svg)](code-of-conduct.md)

The eventzAPI supports a Python micro services infrastructure.
It employs publish and subscribe messaging using AMQP messaging (RabbitMQ) & Event Sourcing.
It provides support for Qt,tkinter or wxPython GUIs.
A Librarian Client supports querying an indelible event store (Archive).

The API requires that a RabbitMQ server be accessible and if a master archive is needed, an Archivist and Librarian available through Docker Hub.

To use eventzAPI in an application example code follows:

-----------------------------------------------------

from eventzAPI import ApplicationInitializer
.
.
Class Main(object):
    def initialize_eventz(self):

        subscriptions = [< A list of event record types that the application subscribes to eg. [250001.00", "250002.00"]>]
        publications = [< A list of event record types that the application publishes eg. [250011.00", "250022.00"]>]
        application_id = < A UUID unique to this application eg.'65aa1c62-0991-48b7-a52c-1140c73582ce'>
        application_name = 'Your application name'
        user_id = 'Your user name if there is one, otherwise an empty string (Needed for ApplicationInitializer)'

	# Instantiate the Application Initializer that will generate the objects needed to access the API's methods
        ai = ApplicationInitializer(subscriptions, publications, application_id, application_name, self.settings_path,
                                    user_id)
	# Get the objects
        self.a_publisher, self.a_subscriber, self.a_logger, self.a_librarian_client, self.eventz_utilities, self.parameters = ai.initialize()
	# The initializer generates a session Id (UUID_) which is included in an event record metadata. Useful in separating events messages 
	# Meant for your instance of the application
        self.my_session_id = str(self.parameters.session_id)

	# Inform the universe that this application instance has started.
        eventz_utilities.start_application(self.a_publisher, user_id)
        pass
-----------------------------------------------------

