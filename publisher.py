# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code based on basic pub/sub but modified for xsub and xpub
#
# We are executing these samples on a Mininet-emulated environment
#
#  Weather update server
#  Publishes random weather updates
#  Connects to a xsub on port 5555
#
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
from random import randrange
import time

zoo_ok = False

class Publisher:
    """Implementation of a publisher"""
    def __init__(self, broker_addr, ownership_strength, history):
        self.broker = broker_addr
        self.strength = ownership_strength
        self.history = history
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        # Connet to the broker
        self.path = '/leader/node'
        #print "connected!"
        self.zk_object = KazooClient(hosts='127.0.0.1:2181') #or to make it multiple zookeepers.....
        self.zk_object.start()

        #initializtion, what for zookeeper_server to be steady
        for i in range(5): # multiple test
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event == None: #wait for event to be alive and None(stable)
                    data, stat = self.zk_object.get(self.path)
                    global zoo_ok
                    zoo_ok = True
        if zoo_ok:   # make sure zoo keeper is ok,
            data, stat = self.zk_object.get(self.path)
            address = data.split(",")
            self.connect_str = "tcp://" + self.broker + ":"+ address[0]
            self.socket.connect(self.connect_str)
        else:
            print("Zookeeper is not ready yet, please restart the publisher later")

    def publish(self,zipcode):
        # Keep publishing
        zipcode = int(zipcode.decode('ascii'))
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
                            self.connect_str = "tcp://" + self.broker + ":"+ address[0]
                            print(self.connect_str)
                            #print ("Publisher connecting to proxy at: {}".format(connect_str))
                            self.socket.connect(self.connect_str)

            temperature = randrange(-80, 135)
            relhumidity = randrange(10, 60)
            #print ("Sending: %i %i %i" % (zipcode, temperature, relhumidity))
            pub_timestamp = time.time()
            self.socket.send_string("%i %i %i %i %i %i" % (zipcode, temperature, relhumidity, self.strength, self.history, pub_timestamp))
            # print("publisher time is: ", pub_timestamp)
            time.sleep(1)

    def close(self):
        """ This method closes the PyZMQ socket. """
        self.socket.close(0)

if __name__ == '__main__':
    strength = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    zipcode = sys.argv[2] if len(sys.argv) > 2 else '10001'
    broker = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
    history = int(sys.argv[3]) if len(sys.argv) > 3 else 7
    print('Topic:',zipcode)
    pub = Publisher(broker,strength, history)
    if zoo_ok:
        print( "--Start publishing--")
        pub.publish(zipcode)