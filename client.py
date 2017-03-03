"""

    This file implements the client behavior: connect to datacenter and ask for ticket
        Send: "BUY $number"
        Receive: "SUCCESS" or "FAIL"

"""

from entity import Entity

class Client(Entity):

    def __init__(self, host_id = None):
        super(Client, self).__init__(host_id)

    # send request to datacenter and wait for response
    def buy(self, ticket_num, host, port, target_id = None):

        connection_obj = self.start_connection(host, port)
        message = "BUY %d" % ticket_num
        self.send_a_message(message, connection_obj)
        response = self.receive_a_message(connection_obj)

        if response == "SUCCESS":
            print "[LOG][%s] %d tickets bought from datacenter %s" % (self.host_id, ticket_num, target_id)
        else:
            print "[LOG][%s] %d tickets denied from datacenter %s" % (self.host_id, ticket_num, target_id)

        connection_obj.close()


if __name__ == '__main__':
    
    import sys

    ticket_num = int(sys.argv[1])
    host = sys.argv[2]
    port = int(sys.argv[3])
    target_id = sys.argv[4]
    if len(sys.argv) >= 6:
        host_id = sys.argv[5]
    else:
        host_id = "Client"

    client_obj = Client(host_id)
    client_obj.buy(ticket_num, host, port, target_id)
