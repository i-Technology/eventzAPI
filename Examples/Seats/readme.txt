Seats Demo

Notes:
1.This application does not render properly on  OSX. Sets do not change colour. However, the log generated notes when seats
change colour.
2. This application requires python version 3.12 for correct ISO date rendering in datetime.


This python script demonstrates the use of eventzAPI. It generates a seat map simulating the client for a ticket selling application. When the user selects a seat, a message is generated that is sent to a publish and subscribe broker (RabbitMQ) The application subscribes to this message and when it receives it, it changes the state (colour) of the seat to yellow. selecting other seats will repeat the process. If the user clicks on the ‘Payment’ button within a timeout period the subsequent message turnaround will cause the seats selected to turn green indicating they have been purchased by the user. If the timeout expires messaging will be sent that causes the seats to revert to an ‘available’ state (grey) As seats are claimed they are added to a list displayed in a text box at the bottom of the map.

A second instance of this application, running on the users’ machine or another users’ machine, will also subscribe to these messages and ,when the other user selects a seat, the seat will turn to red indicating that the other user has claimed the seat and that it is not available to you. Similarly, a released seat will revert to ‘available’ (grey) . Seats claimed by the user will appear in red on the other users’ map. This will be true for any additional instances of the application wherever they run.

If an application is launched after other applications have claimed seats it will send a ‘seats request’ message and the other running applications will publish a ‘seat vector’ message which contains a list of all claimed seats. The new application receives these seat vectors and updates its’ map accordingly.

Code

The application uses tkinter for the gui. The startup code instantiates a tkinter root, an Auditorium object and calls the root.mainloop().
The Auditorium object initialization sets up attributes, initializes the eventz environment, and executes a run() method.

Eventz initialization consists of 
    1. creating a subscriptions list that lists the eventz record identifiers that the application subscribes to.
    2. creating a publication list that lists the eventz record identifiers that the application publishes.
    3. setting the application id (a uuid that uniquely identifies this application).
    4. setting the application name.
    5. setting a user_id.
    6. instantiates an Application Initializer.
    7. executes the Application Initializer initialize() method that generates the following objects:
        a) a Publisher – handles publishing eventz records
        b) a subscriber – a task that watches for subscribed to messages from the broker and passes them on to the main task
        c) a logger
        d) a librarian client – interfaces with the librarian service when one is used. (Not so here)
        e) a utility object – has useful utility methods
        f) a parameters object – holds system attributes needed for operation
       
The run() method:
    1. creates a seat vector
    2. publishes a seat request – so other running applications will publish their seat vectors
    3. creates the gui (seat map)
    4. checks the inter-task queue for any subscribed to messages – if a message arrives the process_message() method handles it. The check_queue method also down counts a purchase period that if zero, returns any claimed seats.

The application require access to a RabbitMQ broker. CloudAMQP (https://www.cloudamqp.com/) provides free brokers that are useful for our purpose. The credentials for an instance of the Broker are placed in the file settings.yaml as follows:

brokerExchange: amq.topic
brokerIP: <Hosts> e.g.codfish-01.rmq.cloudamqp.com 
brokerPassword: <Password>W8WyOQoRRCfAIMw_lExq0h--g38eU7Vy
brokerUserName: <Username>zuklzxqa
brokerVirtual: <Username>zuklzxqa

A complete settings.yaml file is in this repository. use it as an example.

Deployment

    1. Establish a RabbitMQ broker at cloudamqp.com (The free one is fine).
    2. unzip the seats.zip file creating s Seats directory with the relevant files.
    3. Edit the settings.yaml to provide the broker credentials.
    4. With a terminal in the seats directory:
        a) pip install eventzAPI
        b) pip install pika
        c) pip install pyYaml
        d) python seats.py
        e) Buy some seats
    5. With another terminal in the seats directory:
        a) python seats.py
    6. Buy seats in each running instance and see the interaction.


