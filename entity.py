"""

    This file implements socket message passing basics

    Inherit this class to further implement client and datacenter

"""

import socket
import sys
import time

# implement message passing here
class Entity(object):

    def __init__(self, host_id = None):
        super(Entity, self).__init__()
        self.passive_sock = None # accept connection
        self.passive_port = -1
        self.host_id = host_id

    # accept connection
    # set self.passive_sock and start listening when necessary
    # return connection handler
    def wait_a_connection(self, port):
        if self.passive_sock == None:
            self.passive_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.passive_sock.bind( ('', port) )
            self.passive_sock.listen(5)
            self.passive_port = port
        connection_obj, address = self.passive_sock.accept()
        print "[LOG][%s] Receive connection from %s" % (self.host_id, address)
        return connection_obj

    # actively create a connection
    def start_connection(self, host, port):
        address = (host, port)
        connection_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection_obj.connect( address )
        print "[LOG][%s] Create connection to %s" % (self.host_id, address)
        return connection_obj

    # put a message into a connection
    # delay the sending by setting delay > 0 (second)
    #   message is a string, start with numbers only, followed by a '|', indicate number of bytes
    #   then the rest bytes are message body
    def send_a_message(self, message_str, connection_obj, delay = 0):
        message_to_send = str(len(message_str)) + '|' + message_str
        if delay > 0:
            time.sleep( delay )
        connection_obj.sendall(message_to_send)

    # extract a message from a connection
    def receive_a_message(self, connection_obj):
        message_len_str = connection_obj.recv(1)
        while message_len_str[-1] != '|':
            message_len_str += connection_obj.recv(1)
        return connection_obj.recv( int(message_len_str[:-1]) )


if __name__ == '__main__':

    port_constant = 65432

    ############## test socket ##############
    if sys.argv[1] == "s":
        test_entity = Entity('S')
        connection_obj = test_entity.wait_a_connection(port_constant)
        time.sleep(5)
        test_entity.send_a_message("A true warrior does not hide, Poseidon!", connection_obj)
        time.sleep(5)
        test_entity.send_a_message("Leave the sea and face me!", connection_obj)
    elif sys.argv[1] == "c":
        test_entity = Entity('C')
        connection_obj = test_entity.start_connection('wasp.cs.ucsb.edu', port_constant)
        print test_entity.receive_a_message(connection_obj)
        print test_entity.receive_a_message(connection_obj)
