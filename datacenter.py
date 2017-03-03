"""

    This file implements the datacenter behavior: connect to other datacenters and response to ticket requests
        - Datacenter connection: send "DID $host_id", all ids are integers
        - Require: send "REQUEST|$num_tickets|$clock"
        - Reply: send "REPLY|$num_tickets|$clock"
        - Release: send "RELEASE|$num_tickets|$clock"
        - To client: send "SUCCESS" or "FAIL"

    Datacenter info contained in a config file
    Must be deployed in the order of datacenter id

"""

from entity import Entity
import threading

MESSAGE_DELAY = 5 # delay of messages between datacenters, messages between a datacenter and a client have 0 delay

class Datacenter(Entity):

    def __init__(self, host_id = None):
        super(Datacenter, self).__init__(host_id)
        self.tickets_left = -1
        self.clock = 0
        self.clock_mutex = threading.Lock()
        self.connections = {} # {target_id: connection_obj}, for storing all connections to other datacenters

        self.queue = [] # queue for lamport pair
        self.queue_mutex = threading.Lock()
        self.reply_list = [] # when send a request, store the id which has already replied
        self.reply_list_mutex = threading.Lock()
        self.reply_list_full = False
        self.reply_list_full_mutex = threading.Lock()
        self.reply_list_full_mutex.acquire()

        self.client_queue = []
        self.client_queue_mutex = threading.Lock()

        print "[%d][HOST][%s] Datacenter start" % (self.clock, host_id)

    # send REQUEST/REPLY/RELEASE message to a datacenter
    # set clock + 1
    def send_to_datacenter(self, message_type, num_tickets, datacenter_id, send_clock, delay = 0):

        assert message_type == "REQUEST" or message_type == "RELEASE" or message_type == "REPLY"
        message_str = "%s|%d|%d" % (message_type, num_tickets, send_clock)
        print "[%d][HOST][%s->%s] Send %s" % (self.clock, self.host_id, datacenter_id, message_str)
        self.send_a_message(message_str, self.connections[datacenter_id], delay)

    def send_to_datacenters(self, message_type, num_tickets, datacenter_list, send_clock, delay):

        self.clock_mutex.acquire()
        self.clock += 1
        for datacenter_id in datacenter_list:
            arguments = (message_type, num_tickets, datacenter_id, send_clock, delay)
            threading.Thread( target = self.send_to_datacenter, args = arguments ).start()
        self.clock_mutex.release()

    # receive a message from datacenter
    # set clock value properly
    # return: message_type, num_tickets, target_clock
    def receive_from_datacenter(self, datacenter_id):

        message_str = self.receive_a_message(self.connections[datacenter_id])
        self.clock_mutex.acquire()
        split_message = message_str.split("|")
        message_type = split_message[0]
        num_tickets = int(split_message[1])
        target_clock = int(split_message[2])
        self.clock = max(self.clock, target_clock) + 1
        print "[%d][HOST][%s->%s] Receive %s" % (self.clock, datacenter_id, self.host_id, message_str)
        self.clock_mutex.release()
        return message_type, num_tickets, target_clock

    # connect to other datacenter
    #   actively connect to datacenters with smaller ID and send ID message
    # first line of config file indicates total number of tickets
    # rest lines info of datacenters, each line a single datacenter
    #   $id<int> $host<str> $port<int>
    def datacenter_connection(self, config_filename):

        message_str = "DID %s" % (self.host_id)
        fin = open(config_filename)
        self.tickets_left = int(fin.readline().strip())
        for aline in fin:
            split_line = aline.strip().split()
            datacenter_id = int(split_line[0])
            host = split_line[1]
            port = int(split_line[2])
            if datacenter_id < self.host_id: # create a connection
                connection_obj = self.start_connection(host, port)
                self.connections[datacenter_id] = connection_obj
                self.send_a_message(message_str, connection_obj)
                print "[%d][HOST][%s] Connect to datacenter %s" % (self.clock, self.host_id, datacenter_id)
                self.handle_connection( connection_obj, datacenter_id )
            elif datacenter_id == self.host_id:
                self.passive_port = port
        fin.close()

    # when new connection request comes, deal with the new connection
    # if set datacenter_id, then directly call handle_datacenter
    # otherwise, look at the first message
    def handle_connection(self, connection_obj, datacenter_id = None):

        if datacenter_id in self.connections:
            thread_this = threading.Thread( target = self.handle_datacenter, args = (datacenter_id,) )
            thread_this.start()

        else:
            first_message = self.receive_a_message(connection_obj)

            # connection information from another datacenter
            if first_message.startswith("DID"):
                # identify who is the datacenter
                datacenter_id = int(first_message.replace("DID ",""))
                self.connections[datacenter_id] = connection_obj
                print "[%d][HOST][%s] Connect to datacenter %s" % (self.clock, self.host_id, datacenter_id)
                thread_this = threading.Thread( target = self.handle_datacenter, args = (datacenter_id,) )
                thread_this.start()
            # buy ticket request from client
            elif first_message.startswith("BUY"):
                thread_this = threading.Thread( target = self.handle_client, args = (connection_obj,first_message) )
                self.client_queue_mutex.acquire()
                self.client_queue.append( thread_this )
                self.client_queue_mutex.release()
    
    # calculate whether can sell ticket to client
    def handle_client(self, connection_obj, message_str):

        num_tickets = int(message_str.replace("BUY ",""))

        self.clock_mutex.acquire()
        # put self's request into queue
        self.queue_mutex.acquire()
        self.queue.append( ( self.clock, self.host_id) )
        self.queue.sort()
        print "[%d][HOST][%s] Add Request Self Pair(%s, %s) in into queue, Now queue is %s" % (self.clock, self.host_id, self.clock, self.host_id, self.queue)
        self.queue_mutex.release()

        send_clock = self.clock
        self.clock_mutex.release()

        self.send_to_datacenters("REQUEST", num_tickets, self.connections.keys(), send_clock, MESSAGE_DELAY)

        self.reply_list_mutex.acquire()
        self.reply_list = []
        self.reply_list_mutex.release()

        # wait for everyone to reply; nothing happens before receiving all replies
        self.reply_list_full_mutex.acquire()

        while True:
            
            # if the first request in queue belongs to self, try to satisfy the reuqest
            if self.queue[0][1] == self.host_id:
                self.clock_mutex.acquire()
                send_clock = self.clock
                if num_tickets <= self.tickets_left:
                    self.send_a_message("SUCCESS", connection_obj)
                    self.tickets_left -= num_tickets
                    print "[%d][HOST][%s] Sell %d tickets, %d left\n" % (self.clock, self.host_id, num_tickets, self.tickets_left)
                    self.clock_mutex.release()
                    self.send_to_datacenters("RELEASE", num_tickets, self.connections.keys(), send_clock, MESSAGE_DELAY)
                else:
                    self.send_a_message("FAIL", connection_obj)
                    print "[%d][HOST][%s] Deny %d tickets, %d left\n" % (self.clock, self.host_id, num_tickets, self.tickets_left)
                    self.clock_mutex.release()
                    self.send_to_datacenters("RELEASE", 0, self.connections.keys(), send_clock, MESSAGE_DELAY)
                self.queue.pop(0)
                break
            

    # consistently listen and respond to other datacenters
    def handle_datacenter(self, datacenter_id):

        # deal with rest of messages
        connection_obj = self.connections[datacenter_id]
        while True:

            message_type, num_tickets, target_clock = self.receive_from_datacenter(datacenter_id)

            if message_type == "REQUEST":
                self.queue_mutex.acquire()
                self.queue.append( ( target_clock, datacenter_id ) )
                self.queue.sort()
                print "[%d][HOST][%s] Add Request Others' Pair(%s, %s) in into queue, Now queue is %s" % (self.clock, self.host_id, target_clock, datacenter_id, self.queue)
                self.send_to_datacenters("REPLY", num_tickets, [datacenter_id], self.clock, MESSAGE_DELAY)
                self.queue_mutex.release()

            if message_type == "REPLY":
                self.reply_list_mutex.acquire()
                self.reply_list.append( datacenter_id )
                print "[%d][HOST][%s] Receive Reply from Host[%s], Now the reply_list is %s" % (self.clock, self.host_id, datacenter_id, self.reply_list)
                if sorted(self.reply_list) == sorted( self.connections.keys() ):
                    self.reply_list_full_mutex.release()

                self.reply_list_mutex.release()
            
            if message_type == "RELEASE":
                self.queue_mutex.acquire()
                self.queue.pop(0)
                self.tickets_left -= num_tickets
                print "[%d][HOST][%s] Receive Release from Host[%d], Sell %d tickets, %d left \n" % (self.clock, self.host_id, datacenter_id, num_tickets, self.tickets_left)
                self.queue_mutex.release()

    def check_client_queue(self):
        print "START CHECK"
        while True:
            if len(self.client_queue) > 0:
                self.client_queue_mutex.acquire()
                self.client_queue[0].start()
                thread_this = self.client_queue.pop(0)
                self.client_queue_mutex.release()
                thread_this.join()

    # the whole process
    # always waiting for new connections to come in, and fork a thread to deal with it
    def run(self, config_filename):
        
        self.datacenter_connection(config_filename)

        threading.Thread( target = self.check_client_queue, args = () ).start()

        while True:
            # wait for a new connection
            connection_obj = self.wait_a_connection(self.passive_port)
            self.handle_connection(connection_obj)


if __name__ == '__main__':
    
    import sys

    host_id = int(sys.argv[1])
    config_filename = sys.argv[2]

    datacenter_obj = Datacenter(host_id)
    datacenter_obj.run(config_filename)





