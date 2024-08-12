''''
Seats.py
This python script demonstrates the use of eventzAPI. It generates a seat map simulating the client for a ticket selling
application. When the user selects a seat, a message is generated that is sent to a publish and subscribe
broker (RabbitMQ) The application subscribes to this message and when it receives it, it changes the state (colour)
of the seat to yellow. selecting other seats will repeat the process. If the user clicks on the ‘Payment’ button within
a timeout period the sbsequent message turnaround will cause the seats selected to turn green indicating they have been
purchased by the user. If the timeout expires messaging will be sent that causes the seats to revert to an ‘available’
state (grey) As seats are claimed they are added to a list displayed in a text box at the bottom of the map.'''

import tkinter as tk
import json
from time import perf_counter as my_timer

from eventzAPI import ApplicationInitializer


class Auditorium:
    def __init__(self, root, rows=10, cols=15, seat_cost=10, settings_path='settings.yaml'):

        self.root = root
        self.rows = rows
        self.cols = cols
        self.seat_cost = seat_cost
        self.settings_path = settings_path
        self.seats = {}
        self.my_seats = []
        self.my_paid_seats = []
        self.purchase_period = 0
        self.my_session_id = ''

        self.seat_vector = {}

        self.a_publisher = None
        self.a_subscriber = None
        self.a_logger = None
        self.a_librarian_client = None
        self.eventz_utilities = None
        self.parameters = None

        self.message_text = None
        self.message_label = None

        self.initialize_eventz()
        self.run()

    def initialize_eventz(self):
        subscriptions = ["250001.00", "250002.00", "250003.00", "250009.00", "500001.00", "9000000.00"]
        publications = ["250001.00", "250002.00", "250003.00", "250009.00"]
        application_id = '65aa1c62-0991-48b7-a52c-1140c73582ce'
        application_name = 'EventzAPI_Test_1'
        user_id = 'Starting - No user yet.'

        ai = ApplicationInitializer(subscriptions, publications, application_id, application_name, self.settings_path,
                                    user_id)
        self.a_publisher, self.a_subscriber, self.a_logger, self.a_librarian_client, self.eventz_utilities, self.parameters = ai.initialize()
        self.my_session_id = str(self.parameters.session_id)
        pass

    def create_seat_vector(self):
        sv = {}
        for row in range(self.rows):
            for col in range(self.cols):
                key = f"{chr(row + 65)}{col+1}"
                sv[key] = '1'
        return sv

    def create_gui(self):
        # self.root = tk.Tk()
        self.root.title("Auditorium Seating Plan 2OOg " + self.my_session_id[-3:])

        for row in range(self.rows):
            for col in range(self.cols):
                seat_label = f"{chr(row + 65)}{col + 1}"
                status = next((self.seat_vector[seat] for seat in self.seat_vector if seat == seat_label), '1')
                bg_color = "red" if status == '2' else "light gray"
                button = tk.Button(self.root, text=seat_label, bg=bg_color,
                                   command=lambda seat=seat_label: self.book_seat(seat))
                button.grid(row=row, column=col)
                self.seats[seat_label] = button

        for col in range(self.cols):
            self.root.columnconfigure(col, minsize=50)

        payment_button = tk.Button(self.root, text="Payment", width=5, height=3, command=self.payment_pressed)
        payment_button.grid(row=self.rows, column=0, pady=10, columnspan=2, sticky=tk.W + tk.E)

        self.message_text = tk.Text(self.root, height=2, width=50)
        self.message_text.grid(row=10, column=2, columnspan=9)

        self.message_label = tk.Label(self.root, text="Welcome to the keypad GUI!", wraplength=300)
        self.message_label.grid(row=10, column=14, columnspan=4, sticky=tk.W + tk.E)

        self.display_message_label("Hello!")


    def seat_color_setter(self, button, new_status, sid, seat):
        seat_color = "red"  # default
        if new_status == '1':
            seat_color = "light gray"  # unoccupied seats are gray
        elif new_status == '2' and sid == self.my_session_id:
            seat_color = "yellow"  # My seats are yellow, not red
        elif new_status == '3' and sid == self.my_session_id:
            seat_color = "green"  # My paid seats are green
        elif new_status == '2':
            seat_color = "red"  # Seats that are occupied by others are red
        button.configure(bg=seat_color)  # Make the color change now!
        print(f'Seat {seat} changed to: {seat_color}')

    def publish_seat_status(self, seat, newstat, session_id):
        data = {
            "seat_key": seat,
            "status": newstat,
            "sid": session_id
        }
        json_string = json.dumps(data)
        message_as_tuple = (json_string,)
        self.a_publisher.publish(250001.00, message_as_tuple)

    def publish_seat_request(self, session_id):
        data = {
            "sid": session_id
        }
        json_string = json.dumps(data)
        message_as_tuple = (json_string,)
        self.a_publisher.publish(250003.00, message_as_tuple)

    def publish_seat_vector(self):
        data = {
            "sid": self.my_session_id,
            "seat_vector": self.seat_vector
        }
        json_string = json.dumps(data)
        message_as_tuple = (json_string,)
        print("114", type(message_as_tuple), message_as_tuple)
        pmess = self.a_publisher.publish(250002.00, message_as_tuple)
        print("116", pmess)
        pass

    def return_my_seats(self):
        for seat in self.my_seats:
            self.publish_seat_status(seat, '1', self.my_session_id)  # newstat = '1'
            print("145c publish)")
        if self.my_seats:  # check if there is any seat waiting for purchase
            self.display_message('Sorry, your seats expired.')
        self.my_seats = []  # empty basket

    def payment_pressed(self):
        total_seats = len(self.my_seats)
        total_cost = total_seats * self.seat_cost  # Assuming each seat costs $10
        print("Total Seats:", total_seats, ", Total Cost: $", total_cost)

        for seat in self.my_seats:
            try:
                self.seat_vector[seat] = 3         # Mark as a Paid Seat
                # self.seat_vector[self.seat_vector.index((seat, '2'))] = (seat, '3')  # Mark as a Paid Seat
            except:
                print('EXCEPTION at line 89!', seat)

            self.publish_seat_status(seat, '3', self.my_session_id)  # Publish the update
            print("162c publish)")
            self.my_paid_seats.append(seat)

        seats_list = list(self.my_paid_seats)  # Convert to a list
        dmm = ', '.join(seats_list)
        dm = f"My paid seats: {dmm}"
        self.display_message('Thanks for your purchase!')
        self.display_message_label(dm)
        self.my_seats = []  # Null list now.

    def display_message(self, message):
        self.message_text.configure(state='normal')  # Enable editing the text box
        self.message_text.insert(tk.END, message + '\n')
        self.message_text.see(tk.END)  # Scroll to the end of the text box
        self.message_text.configure(state='disabled')  # Disable editing the text box

    def display_message_label(self, message):
        if self.message_label:
            self.message_label.config(text=message)

    def book_seat(self, seat):

        current_status = self.seat_vector[seat]
        #if current_status == '1':
        newstat = '2'       # Advance seat status from "available" to "occupied"

        if current_status == '3':
            self.display_message_label('Sorry this seat is Paid already.')
            return

        if current_status == '2':

            if seat in self.my_seats:  # Yes it's mine!
                if self.seat_vector == '2':
                    self.seat_vector[seat] = '1'    # return this seat to "available"

                self.seats[seat].configure(bg="light gray")
                print(f"Seat {seat} is now available again")
                self.seats[seat].update()  # Update the seat button in the map
                self.my_seats.remove(seat)

                newstat = '1'
                seats_list = list(str(self.my_seats))  # Convert to a list
                dmm = ', '.join(seats_list)
                dm = f"My seats: {dmm}"
                self.display_message(dm)
                self.publish_seat_status(seat, newstat, self.my_session_id)  # Publish the update
                print ("207c publish)")

            else:
                self.display_message('Sorry, this seat is taken already.')
        else:
            if self.seat_vector[seat] == '1':
                self.seat_vector[seat] = '2'  # mark the seat as selected (yellow) for 10 sec countdown to purchase

            self.my_seats.append(seat)      # add this seat to the list of my seats.
            seats_list = list(self.my_seats)  # Convert to a list
            dmm = ', '.join(seats_list)
            dm = f"My seats: {dmm}"
            self.display_message(dm)        # display this message on the screen

            self.purchase_period = 1000  # 10 seconds to expiry! Buy soon!
            self.publish_seat_status(seat, newstat, self.my_session_id)  # Publish the update to this seat status!
            print("224c publish)")
        if seat == 'A15':  # A15 is a magical key that clears all sold seats
            for seat in self.seat_vector:
                if self.seat_vector[seat] != '1':      # if the seat is not available (elected or sold)
                    self.publish_seat_status(seat, '1', self.my_session_id)  # newstat = '1'
                    print("229c publish)")
            self.publish_seat_status('A15', '1', self.my_session_id)  # Clean out seat A15
            print("231c publish)")
            self.my_seats = []  # empty basket
            self.my_paid_seats = []  # List of my paid seats

        if seat == 'J15':  # J15 is a magical key that selects seats J1 - J15 and reports the elapsed time, for benchmarking.
            if self.seat_vector ['J15'] == 2:       # If selected already
                print('J15 STATUS IS: Unavailable')
                self.display_message('J15 STATUS IS: Unavailable')

            start_timer = my_timer()        # Test metrics timer
            xx = ('J1', 'J2', 'J3', 'J4', 'J5', 'J6', 'J7', 'J8', 'J9', 'J10', 'J11', 'J12', 'J13', 'J14', 'J15')
            for ii in xx:
                self.my_seats.append(ii)
                self.publish_seat_status(ii, '2', self.my_session_id)
                print("245c publish)")
            seats_list = list(self.my_seats)  # Convert to a list
            dmm = ', '.join(seats_list)
            dm = f"My seats: {dmm}"
            self.display_message(dm)

            end_timer = my_timer()
            print("Elapsed time = {:03,.3f}".format(end_timer - start_timer), "seconds")

    def process_message(self, record_type, message1):
        if message1[0] != "250009.00":  # Ignore the heartbeat message!
            if record_type == "250001.00":
                data = message1[16]
                try:
                    msg_data = json.loads(data)
                except json.decoder.JSONDecodeError:
                    print(f"Error: Invalid JSON data received: {data}")

                seat_key = msg_data.get('seat_key')
                new_status = msg_data.get('status')
                seat_sid = msg_data.get('sid')
                self.seat_vector[seat_key] = new_status
                a_button = self.seats[seat_key]
                self.seat_color_setter(a_button, new_status, seat_sid, seat_key)
                print("261 seat_vector ", type(self.seat_vector), self.seat_vector)

            elif record_type == "250002.00":  # Is this a seat list message?
                data = message1[16]
                print("318", type(data),
                      data)  # Print the type and content of data for debugging; data is a JSON string

                try:
                    msg_data = json.loads(data)         # Make a dictionary
                    #data['seat_vector'] = [tuple(seat) for seat in data['seat_vector']]  # Convert lists back to tuple
                    seat_status_data = msg_data["seat_vector"]

                    sid = msg_data.get("sid")           # this is the sid of the seat list sender (NOT my_sid).

                    # if isinstance(seat_status_data, list) and seat_status_data != []:   # Omit if an empty list
                    if seat_status_data != {}:  # Omit if an empty list

                        for a_seat in seat_status_data:  # test all 170 seats and change when necessary
                            seat = a_seat       # select a seat
                            status = seat_status_data[seat]      # status of this seat
                            button = self.seats[seat]

                            if button:
                                self.seat_color_setter(button, status, sid, seat)

                        # self.root.update()

                except json.decoder.JSONDecodeError:
                    print(f"Error: Invalid JSON data received: {data}")

            elif record_type == "250003.00":  # If this is a request for my seat list
                data = message1[16]
                try:
                    msg_data = json.loads(data)
                    sid = msg_data.get("sid")
                    if sid != self.my_session_id:
                         self.publish_seat_vector()
                except json.decoder.JSONDecodeError:
                    print(f"Error: Invalid JSON data received: {data}")

            elif record_type == "9000000.00":
                pass

    def run(self):
        # self.initialize_eventz()
        self.seat_vector = self.create_seat_vector()

        self.publish_seat_request(self.my_session_id)       # 003.00 request message

        self.create_gui()

        sleep_counter = 0
        self.check_queue()

    def check_queue(self):

        if not self.parameters.inter_task_queue.empty():
            message = self.parameters.inter_task_queue.get_nowait()
            print(f'Received: {message}')
            self.parameters.inter_task_queue.task_done()
            self.process_message(message[0], message)
        else:
            if self.purchase_period > 0:  # positive number?
                self.purchase_period -= 1  # less time is left
                if self.purchase_period == 0:  # If no time is left
                    self.return_my_seats()  # Your seats return to the pool

        self.root.after(100, self.check_queue)


if __name__ == "__main__":
    root = tk.Tk()
    auditorium = Auditorium(root)
    root.mainloop()
    pass
