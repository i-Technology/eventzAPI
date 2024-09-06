
from eventzAPI.eventzAPI import DS_Logger, RecordAction, ApplicationInitializer
import atexit
import tkinter
from tkinter import ttk, BOTH, RAISED, messagebox
from tkinter.ttk import ( Button, Frame, Style)
import time
import sys

'''
    This Hello World application demonstrates the use of the eventzAPI library. It launches a small window with one
    button "Hello World". Pressing this button publishes a Hello World eventz message that is sent to the RabbitMQ
    broker specified in the settings.yaml file. The application also subscribes to this message so when the broker
    receives it it sends it back to the application. Upon receipt, the application displays the complete message in a 
    message box. The message consists of a 16 item metadata piece followed by the "Hello World" string as the payload.
    
    To run this program you need python3.8 or higher and eventzAPI (pip3 install eventzapi)'''
#
# Main Object
#

class Main(ttk.Frame):
    # Object constructor
    def __init__(self, parent, ds_param, user_Id, librarian_client, utilities, subscriber):
        # Attributes
        # Define any attributes needed for the application here
        self.style = Style()
        self.inter_taskQ = ds_param.inter_task_queue
        self.user_Id = user_Id
        # Get the Publisher object to enable publishing DS messages
        self.my_publisher = ds_param.the_publisher
        # Create a DS_Logger object to enable DS Logging which publishes log messages
        my_logger = DS_Logger(ds_param)
        # Create a Librarian Client object to send queries to the Librarian
        # librarian_client = LibrarianClient(ds_param, my_logger)
        # Create a DS_Utility object to allow access to DS specific utility methods
        self.my_utility = utilities
        self.my_utility.start_application(self.my_publisher, self.user_Id)     # Publish the fact that this app is starting
        # Instantiate an Archiver (Local)
        archiver = None
        # Create and start a subscriber thread
        # a_subscriber = subscriber
        self.subscriber = subscriber
        # self.subscriber.start()
        # GUI
        ttk.Frame.__init__(self, parent)
        self.root = parent
        self.init_gui()

        self.say_hello()
        # Handle exiting the app.
        atexit.register(self.stopping)
        self.root.protocol("WM_DELETE_WINDOW", self.stopping)
    # Code to execute when the application stops
    def stopping(self):
        print('Stopping! The Subscriber')
        self.subscriber.stop()  # Stop the subscriber task too
        print('Stopping! The Application')
        self.root.destroy()
    # Subscriber initialization sample
    def init_gui(self):

        """Builds GUI."""
        self.root.title('Hello')
        self.root.geometry('300x100+10+20')

        self.style.theme_use("default")

        frame = Frame(self, relief=RAISED, borderwidth=1)
        frame.pack(fill=BOTH, expand=True)
        self.pack(fill=BOTH, expand=True)

        self.hello_button = Button(self.root, text = 'Say Hello', command=lambda: self.say_hello())
        self.hello_button.pack(padx = 5, pady = 5)
        pass

    def say_hello(self):
        message = ('Hello World',)
        messagePublished = self.my_publisher.publish(50000.00, message, RecordAction.INSERT.value, userId=self.user_Id)
        pass

    def show_response(self, response):
        messagebox.showinfo('Response', response)
#
# Main code executed at the beginning
#
if __name__ == "__main__":

    # Set the application parameters
    application_id = '8ea2d01a-ae28-4f16-856e-aa8ccdd34b43'    # Identifies the application. Set by developer and only changed with version
    application_name = 'Hello World'
    path_to_settings = sys.argv[1]
    subscriptions = ['50000.00']   # Record Types subscribed to
    publications = ['50000.00']      # Record Types published
    user_id = 'You' # This user name is included in the metadata for any message published

    # Initialize to get eventz objects
    ai = ApplicationInitializer(subscriptions, publications, application_id, application_name,
                                         path_to_settings, user_id)
    a_publisher, subscriber, logger, librarian_client, utilities, parameters = ai.initialize()

    root = tkinter.Tk()         # Set up GUI

    # Instantiate your Main object. This name changes when you refactor
    main = Main(root, parameters, user_id, librarian_client, utilities, subscriber)

    # Process Loop
    root.update()
    while len(root.children) != 0:
        root.update_idletasks()
        root.update()
        # Watch for and handle messages from the Subscriber Task
        if parameters.inter_task_queue.empty() == False:
            message = parameters.inter_task_queue.get_nowait()   # Get the message - non blocking
            print('Message Received: {}'.format(message))
            parameters.inter_task_queue.task_done()  # Let the queue know the message was handled
            # Put code here to handle the message
            if message[0] == '50000.00':
                main.show_response(message)
                pass
        else:
            time.sleep(.1)  # Wait to prevent the polling from using up cpu resources
