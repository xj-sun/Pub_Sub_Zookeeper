# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
#
#   Weather update client
#   Connects SUB socket to tcp://localhost:5556
#   Collects weather updates and finds avg temp in zipcode
from kazoo.client import KazooClient
from kazoo.client import KazooState

import logging

from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)

logging.basicConfig() # set up logginga

import sys
import zmq
import time

zoo_ok = False
class Subscriber:
    """Implementation of the subscriber"""
    def __init__(self, broker_addr, new_port, zipcode):
        self.broker = broker_addr
        self.zipcode = zipcode
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        # Connect to broker
        self.path = '/leader/node'
        self.zk_object = KazooClient(hosts='127.0.0.1:2181') #or to make it multiple zookeepers.....
        self.zk_object.start()
        self.port = new_port
        if self.port:
            #for new subscribers to get the history vector
            self.history_path = "/history/"
            self.history_node = self.history_path + "node"
            send_string = self.broker + "," + self.port
            if self.zk_object.exists(self.history_node):
                pass
            else:
                # Ensure a path, create if necessary
                self.zk_object.ensure_path(self.history_path)
                # Create a node with data
                self.zk_object.create(self.history_node, ephemeral = True)
            self.zk_object.set(self.history_node, send_string)
            self.connect_str = "tcp://" + self.broker + ":"+ self.port
            self.socket.connect(self.connect_str)
            # any subscriber must use the SUBSCRIBE to set a subscription, i.e., tell the
            # system what it is interested in
            self.socket.setsockopt_string(zmq.SUBSCRIBE, self.zipcode)
            global zoo_ok
            zoo_ok = True
        else:
            #initializtion, what for zookeeper_server to be steady
            for i in range(5): # multiple test
                @self.zk_object.DataWatch(self.path)
                def watch_node(data, stat, event):
                    if event == None: #wait for event to be alive and None(stable)
                        data, stat = self.zk_object.get(self.path)
                        global zoo_ok
                        zoo_ok = True
            if zoo_ok:   # make sure zookeeper is steady
                data, stat = self.zk_object.get(self.path)
                address = data.split(",")
                self.connect_str = "tcp://" + self.broker + ":"+ address[1]
                self.socket.connect(self.connect_str)
                # any subscriber must use the SUBSCRIBE to set a subscription, i.e., tell the
                # system what it is interested in
                self.socket.setsockopt_string(zmq.SUBSCRIBE, self.zipcode)
            else:
                print ("Zookeeper is not ready yet, please restart the subscriber later")

    def subscribe(self):
        # Keep subscribing
        while True:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event != None:
                        print(event.type)
                        if event.type == "CHANGED": #reconnect once the election happend, change to the new leader
                            self.socket.close()
                            self.context.term()
                            time.sleep(1)
                            self.context = zmq.Context()
                            self.socket = self.context.socket(zmq.PUB)
                            # Connet to the broker
                            data, stat = self.zk_object.get(self.path)
                            address = data.split(",")
                            self.connect_str = "tcp://" + self.broker + ":"+ address[1]
                            print(self.connect_str)
                            #print ("Publisher connecting to proxy at: {}".format(connect_str))
                            self.socket.connect(self.connect_str)

            string = self.socket.recv_string()
            zipcode, temperature, relhumidity, ownership, history, pub_time = string.split()
            print(history)
            # total_temp += int(temperature)
            pub_time = float(pub_time.decode('ascii'))
            time_diff = time.time() - pub_time
            print ("The time difference is: ", time_diff)
            print (string)

    def close(self):
        """ This method closes the PyZMQ socket. """
        self.socket.close(0)

if __name__ == '__main__':
    zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"
    broker = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    port = sys.argv[3] if len(sys.argv) > 3 else ""
    print ('Topic:',zipcode)
    # Python 2 - ascii bytes to unicode str
    if isinstance(zipcode, bytes):
        zipcode = zipcode.decode('ascii')
    sub = Subscriber(broker, port, zipcode)
    if zoo_ok:
        sub.subscribe()