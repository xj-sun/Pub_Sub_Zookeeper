# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
from kazoo.client import KazooClient
from kazoo.client import KazooState

import logging

from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)

logging.basicConfig()  # set up logginga

import os
import sys
import time
import threading
import zmq
from random import randrange


class Proxy:
    """Implementation of the proxy"""

    def __init__(self):
        # Use XPUB/XSUB to get multiple contents from different publishers
        self.context = zmq.Context()
        self.xsubsocket = self.context.socket(zmq.XSUB)
        self.xpubsocket = self.context.socket(zmq.XPUB)
        self.xpubsocket.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.xpubsocket.send_multipart([b'\x01', b'10001'])
        # Now we are going to create a poller
        self.poller = zmq.Poller()
        self.poller.register(self.xsubsocket, zmq.POLLIN)
        self.poller.register(self.xpubsocket, zmq.POLLIN)
        self.sub_his_dic = {}

        self.topic_info_queue = []  # the content queue for different topic (zipcode)
        self.topicInd = 0
        self.zip_list = []  # the ziplist to keep track with the zipcodes received

        self.zk_object = KazooClient(hosts='127.0.0.1:2181')
        self.zk_object.start()

        self.path = '/home/'

        znode1 = self.path + "broker1"
        if self.zk_object.exists(znode1):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode1, "5555,5556")
            # Print the version of a node and its data

        znode2 = self.path + "broker2"
        if self.zk_object.exists(znode2):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode2, "5557,5558")

        znode3 = self.path + "broker3"
        if self.zk_object.exists(znode3):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode3, "5553,5554")

        self.election = self.zk_object.Election(self.path, "leader")
        leader_list = self.election.contenders()
        self.leader = leader_list[-1].encode('latin-1')  # the leader here is a pair of address

        address = self.leader.split(",")
        pub_addr = "tcp://*:" + address[0]
        sub_addr = "tcp://*:" + address[1]
        print("Current elected broker: ", pub_addr + "," + sub_addr)
        self.xsubsocket.bind(pub_addr)
        self.xpubsocket.bind(sub_addr)

        self.watch_dir = self.path + self.leader  # use  Datawatch

        self.leader_path = "/leader/"
        self.leader_node = self.leader_path + "node"
        if self.zk_object.exists(self.leader_node):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.leader_path)
            # Create a node with data
            self.zk_object.create(self.leader_node, ephemeral=True)
        self.zk_object.set(self.leader_node, self.leader)

    def history_vector(self, h_vec, ind, history, msg):
        if len(h_vec[ind]) < history:
            h_vec[ind].append(msg)
        else:
            h_vec[ind].pop(0)
            h_vec[ind].append(msg)
        return h_vec

    def sendToSubscriber(self):
        events = dict(self.poller.poll(10000))
        # Is there any data from publisher?
        if self.xsubsocket in events:
            msg = self.xsubsocket.recv_multipart()
            content = msg[0]
            zipcode, temperature, relhumidity, ownership, history, pub_time = content.split(" ")

            # if there is a new topic add the topic to the topic list and initialize the history vector fot this topic
            # if the topic already in the topic list update the information of the topic if the ownership is larger than the current ownership
            # update history vector fot the topic

            if zipcode not in self.zip_list:
                self.zip_list.append(zipcode)
                # for this topic, set initial informations for the ownership and history function
                cur_strength = 0
                pre_strength = 0
                count = 0
                history_vec = []
                strengh_vec = []
                pubInd = 0
                pre_msg = []
                cur_msg = []

                topic_info = [cur_strength, pre_strength, count, history_vec, strengh_vec, pubInd, pre_msg, cur_msg]
                self.topic_info_queue.append(topic_info)
                # start to collect the msg for the new topic
                topic_msg, histry_msg, ownership, strengh_vec = self.scheduleInTopic(
                    self.topic_info_queue[self.topicInd], msg)
                self.topicInd += 1
            else:
                zipInd = self.zip_list.index(zipcode)
                topic_msg, histry_msg, ownership, strengh_vec = self.scheduleInTopic(self.topic_info_queue[zipInd], msg)
            zipInd = self.zip_list.index(zipcode)
            msg = self.topic_info_queue[zipInd][3][0]
            msg  = topic_msg + msg
            carry = []
            for contex in msg:
                carry.append(contex[0])
                carry.append('/')

            carry = list(carry[2:])

            res = ['']
            for _ in carry:
                res[-1] += _
            self.xpubsocket.send_multipart([res[0]])  # send the message by xpub

        if self.xpubsocket in events:  # a subscriber comes here
            msg1 = self.xpubsocket.recv_multipart()
            self.xsubsocket.send_multipart(msg1)

    def scheduleInTopic(self, info, msg):
        [cur_strength, pre_strength, count, history_vec, strengh_vec, pubInd, pre_msg, cur_msg] = info

        sample_num = 10
        content = msg[0]
        zipcode, temperature, relhumidity, ownership, history, pub_time = content.split(" ")

        ownership = int(ownership.decode('ascii'))
        history = int(history.decode('ascii'))

        # creat the history stock for each publisher, should be FIFO
        # if the ownership is new add the topic information to history vector
        # otherwise update the information
        if ownership not in strengh_vec:
            strengh_vec.append(ownership)
            # create list for this publisher
            history_vec.append([])
            history_vec = self.history_vector(history_vec, pubInd, history, msg)
            pubInd += 1  # the actual size of the publishers
        else:
            curInd = strengh_vec.index(ownership)
            history_vec = self.history_vector(history_vec, curInd, history, msg)

        # get the highest ownership msg to register the hash ring, using a heartbeat listener
        if ownership > cur_strength:
            pre_strength = cur_strength
            cur_strength = ownership
            pre_msg = cur_msg
            cur_msg = msg
            count = 0
        elif ownership == cur_strength:
            cur_msg = msg
            count = 0
        else:
            count = count + 1
            if count >= sample_num:
                cur_strength = pre_strength
                cur_msg = pre_msg
                count = 0

        # update the info vector fro this topic to make sure the current information always comes from the publisher with strongest ownership
        info[0] = cur_strength
        info[1] = pre_strength
        info[2] = count
        info[3] = history_vec
        info[4] = strengh_vec
        info[5] = pubInd
        info[6] = pre_msg
        info[7] = cur_msg

        # get the history vector for msg
        histInd = strengh_vec.index(cur_strength)
        histry_msg = history_vec[histInd]

        return cur_msg, histry_msg, ownership, strengh_vec

    def schedule(self):
        while True:
            @self.zk_object.DataWatch(self.watch_dir)
            def watch_node(data, stat, event):
                if event != None:
                    print(event.type)
                    if event.type == "DELETED":  # redo a election
                        self.election = self.zk_object.Election(self.path, "leader")
                        leader_list = self.election.contenders()
                        self.leader = leader_list[-1].encode('latin-1')

                        self.zk_object.set(self.leader_node, self.leader)

                        self.xsubsocket.unbind(self.current_pub)
                        self.xpubsocket.unbind(self.current_sub)

                        address = self.leader.split(",")
                        pub_addr = "tcp://*:" + address[0]
                        sub_addr = "tcp://*:" + address[1]

                        self.xsubsocket.bind(pub_addr)
                        self.xpubsocket.bind(sub_addr)

            self.election.run(self.sendToSubscriber)

    def close(self):
        """ This method closes the PyZMQ socket. """
        self.xsubsocket.close(0)
        self.xpubsocket.close(0)


if __name__ == '__main__':
    proxy = Proxy()
    proxy.schedule()