import time
import socket
import threading
import random
import pickle
from logger import logger
import os
import sys

class Node():
    def __init__(self, pml=None):
        super().__init__()
        self.membership_list = {}                                           # MemberClass()
        self.heartbeat_counter = 0                                          # heartbeat counter
        self.ip = socket.gethostbyname(socket.gethostname())
        self.last_changed_id = 0
        self.style = threading.Event()                                      # GOSSIP or ALL TO ALL, intially All to All
        self.style.set()
        self.leaving = threading.Event()                                    # whether node is leaving or not
        self.joined = threading.Event()                                     # when node has joined or not
        self.halted = threading.Event()                                     # whether this node has to halt of not
        self.halted_complete = threading.Event()                            # whether halting is complete or not
        self.removal_node_event = threading.Event()                         # removal event
        self.introducer_ip_address = "172.22.158.65"                        # set the IP address of the introducer
        if(self.ip != self.introducer_ip_address):                          # whether node is the introducer or not
            self.is_introducer = False
        else:
            self.is_introducer = True
        self.gossip_period = 0.4                                            # define the gossip period
        self.cleanup_period = 1                                             # define time before which it needs to clean up
        self.failure_period = 2                                             # define time before which it needs to judge whether the node failed or not
        self.UDP_PORT = 12345                                               # defines the UDP port
        self.failure_list = set()                                           # Define failure list
        self.lock = threading.Lock()
        self.gossip_elem = 3                                                # Define number of elements to which heartbeat is sent in gossip
        self.removal_node = None                                            # initially none, used in halting node
        self.wait_join_time = 1                           # how much time to wait before trying joining again
        # self.pml = [0]*0 + [1]*(100-0)
        # self.val = pml
    # The next few functions are for starting/stopping the node(s)
    '''
    Input: None
    Purpose: Initialize sockets for sending / receiving data
    Side effects: Sets up sockets for this machine
    Output: None
    '''

    def start_node(self):
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.UDP_PORT))
        self.membership_list[self.ip] = (time.time(), self.heartbeat_counter, time.time())
        logging.info("Node started")
        
    '''
    Input: None
    Purpose: Initialize sockets for sending / receiving data
    Side effects: Sets up sockets for this machine
    Output: None
    '''
    def join_group(self):
        number_times = 3
        while(number_times):
            self.send_data(pickle.dumps(['I want to join', self.heartbeat_counter]), self.introducer_ip_address)
            if(self.joined.is_set()):
                logging.info ("Node joined successfully")
                return True
            time.sleep(self.wait_join_time)
            number_times -= 1

        logging.info ("Node failed to join")    
        return False
    
    def remove_unecessary_variables(self):
        self.lock.acquire()
        for elem in self.failure_list:
            try:
                self.membership_list.pop(elem)
            except KeyError:
                continue
        self.membership_list.pop(self.ip)    
        self.lock.release()

    '''
    Input: ID of node (which can be None)
    Purpose: Halting a particular node
    Side effects: Stops the particular node (closes the socket), makes it leave the group
    Output: True if succesful, else false
    '''

    def halt_node(self, crash=None):
        
        self.halted.set()
        self.remove_unecessary_variables()
        self.lock.acquire()
        items = set(self.membership_list.keys())
        self.lock.release()
        
        while(items):
            item = items.pop()
            self.removal_node_event.clear()
            self.removal_node = item
            current_time = time.time()
            while(not(self.removal_node_event.is_set()) and time.time() - current_time <= self.failure_period): 
                self.send_data(pickle.dumps(["I am leaving",]), item)
                time.sleep(self.failure_period/6)
        try:    
            logging.info("I have been terminated")
        except OSError:
            logging.warning ("OS ERROR: unsuccessful halt")
                    

    '''
    Input: ?
    Purpose: ?
    Side effects: ? 
    Output: ?
    '''
    def connection_thread(self):
        raise NotImplementedError


    # The next few functions are handling/receiving/sending data to/from the node(s)
    '''
    Input: Data packet to be sent, ID (IP Address) of the node to send data to  
    Purpose: Send data packet to a specific node
    Side effects: None
    Output: None
    '''

    def send_data(self, packet, id):
        self.sock.sendto(packet, (id, self.UDP_PORT))
        
    '''
    Input: ?  
    Purpose: ?
    Side effects: ?
    Output: ?
    '''

    def fetch_data(self, id):
        raise NotImplementedError

    '''
    Input: Data packet assuming in list form, address from which received the packet
    Purpose: Handle data packet received from another node
    Side effects: Membership list might get changed, node might get stopped
    Output: True if packet handled successfully, else false
    '''

    def handle_data(self, packet, address=None):
        logging.info(str(packet[0]) + " received from: " + str(address))
        if(self.halted.is_set()):
            if(packet[0] == "Ok" and address == self.removal_node):
                self.removal_node_event.set()
                logging.info ("Node has acknowledged to remove itself")
                if(len(list(self.membership_list.items())) == 1):
                    self.halted_complete.set()
                return True
            else:
                return False
        try:
            if(packet[0] == "MembershipList"):
                if(not(self.joined.is_set())):
                    self.joined.set()
                if(packet[2][1] > self.last_changed_id):
                    if(packet[2][0]):
                        self.style.set()
                        logging.info("Changed style to Gossip")
                    else:
                        self.style.clear()
                        logging.info("Changed style to AllToAll")
                        
                    self.last_changed_id = packet[2][1]
                self.update_list(packet[1])
            elif(packet[0] == "You leave"):
                self.halt_node()
            elif(packet[0] == "I am leaving"):
                logging.info("Node " + str(address) + " has left")
                self.delete_element(address)
                self.send_data(pickle.dumps(["Ok",]), address)
            elif(packet[0] == "Switch Style"):
                try:
                    self.switch_style(packet[1])
                except IndexError:
                    self.switch_style()
                finally:
                    logging.info("Switching style")
            elif(packet[0] == "I want to join" and self.is_introducer):
                try:
                    if(self.add_element(address, packet[1])):
                        self.send_data(pickle.dumps(["MembershipList", self.membership_list, (self.style.is_set(), self.last_changed_id)]), address)
                        logging.info("Node " + str(address) + " has joined")
                except IndexError:
                    logging.warning("Introducer -- failed join from: " + str(address))
            else:
                logging.warning("Data not recognized")
                return False        
        except IndexError:
            logging.warning ("Invalid packet received")
            return False
        
        return True


    '''
    Input: IP address of node to be removed or the key to membership list
    Purpose: Delete the element of ID from membership list
    Side effects: Removes node from membership list
    Output: None
    '''
    def delete_element(self, id):
        logging.info("Removing element: " + id)
        self.lock.acquire()
        try:
            self.membership_list.pop(id)[0]
        finally:
            try:
                self.failure_list.remove(id)
            except KeyError:
                pass
            finally:
                self.lock.release()
            
    '''
    Input: IP address of node to be removed or the key to membership list
    Purpose: Delete the element of ID from membership list
    Side effects: Removes node from membership list
    Output: None
    '''
    def add_element(self, id, element):
        self.lock.acquire()
        added = True
        try:
            self.membership_list[id] = (time.time(), element, time.time())
            logging.info("Element added")
        except IndexError:
            added = False
            logging.info("Element could not be added")
        finally: 
            self.lock.release()
        return added

    # The next few functions are for switching heartbeating style
    '''
    Input: style(string or int) to change the distributed system to
    Purpose: Change the style of our DS from gossip to all to all or vice versa
    Side effects: Style of our DS might change
    Output: String (with no use, but for logging purposes) that returns information regarding what
            the style was changed to. 
    '''

    def switch_style(self, style=None):
        self.last_changed_id += 1
        if (style == None):
            if (self.style.is_set()):
                self.style.clear()
            else:
                self.style.set()
        else:
            if (style == "Gossip"):
                self.style.set()
            elif (style == "All to All" or style != "Gossip"):
                self.style.clear()
        

    # The next few functions are for updating the membership list & detecting failures
    '''
    Input: Membership List
    Purpose: Update the membership list
    Side effects: 
    Output: None
    '''
    def update_list(self, membership_list):

        self.lock.acquire()
        try:
            for key, value in membership_list.items():
                if(key not in self.membership_list):
                    if(value[1] != "failed"):
                        self.membership_list[key] = (value[0], value[1], time.time())
                        logging.info("Got new element")
                    continue
                
                if(key == self.ip):
                    continue
                elif(key in self.failure_list):
                    continue
                else:
                    if(value[1] == "failed"):
                        logging.info("Received information about failed element")
                        self.failure_list.add(key)
                        self.membership_list[key] = (self.membership_list[key][0], "failed", time.time())
                        continue
                    else:
                        if(self.membership_list[key][1] < value[1]):
                            self.membership_list[key] = (value[0], value[1], time.time())
                            logging.info("Updated node " + str(key) + "'s heartbeat")
        finally:
            self.lock.release()

    '''
    Input: Last updated timestamp
    Purpose: Update the membership list
    Side effects: Might potentially change the membership list
    Output: True if failure detected, else false
    '''
    def check_failure(self, last_updated_time):
        if (time.time() - last_updated_time > self.failure_period):
            return True
        return False

    # The next few functions are for deciding the receivers from the membership list
    '''
    Input: None
    Purpose: Decide the receivers from the membership list
    Side effects: None
    Output: List of IDs of receivers to send the message to
    '''
    def decide_receivers(self):
        
        receivers_list = list(self.membership_list.copy().keys())
        receivers_list.remove(self.ip)
        for elem in self.failure_list:
            try:
                receivers_list.remove(elem)
            except ValueError:
                continue
        
        if (self.style.is_set()):
            try:
                return random.sample(receivers_list, k=self.gossip_elem)
            except ValueError:
                return receivers_list
        else:
            return receivers_list
            

    # Running the node
    '''
    Input: None
    Purpose: Core of the node, essentially run it, listen for data and everything
    Output: None
    '''
    def failure_handling(self):
        while(True):
            if(self.halted.is_set()):
                return
            # if any node fails print the membership list
            self.lock.acquire()
            try:
                membership_list = self.membership_list.copy()
                for key, value in membership_list.items():
                    if(key == self.ip):
                        continue
                    if(key in self.failure_list):
                        if(time.time() - value[2] >= self.cleanup_period):
                            self.failure_list.remove(key)
                            self.membership_list.pop(key)
                    elif(self.check_failure(value[2])):
                        logging.warning("Node with address " + str(key) + " failed")
                        self.membership_list[key] = (self.membership_list[key][0], "failed", time.time())
                        self.failure_list.add(key)
            finally:
                self.lock.release()
                time.sleep(self.failure_period/6)
                

    # types of messages -
    # 1. start/leave request - for introducer
    # 2. membership list - gossip
    # 3. message - all to all heartbeat
    # All to all heart beating - when message is received from a node, its time should be updated in the membership list.
    # while True :
    #   parse message type
    # if heartbeat - send to heartbeat message handler
    # if join, add the node to membership list
    # if leave, remove from membership list
    # if gossip, send to gossip message handler
    # join acknowledgement
    # switch_style acknowledgement
    # self.socket.listen()
    def receive_messages(self):
        while(True):
            if(self.halted_complete.is_set()):
                return
            data, address = self.sock.recvfrom(4096)
            data = pickle.loads(data)
            self.handle_data(data, address[0])
            
    def send_heartbeat(self):
        
        while(True):
            
            if(self.halted.is_set()):
                return
            
            nodes_to_send = self.decide_receivers()
            if(len(nodes_to_send) > 0):
                logging.info("Sending data to " + str(len(nodes_to_send)) + "nodes")
                self.heartbeat_counter = self.heartbeat_counter + 1
            else:
                time.sleep(self.gossip_period)
                continue
            self.membership_list[self.ip] = (self.membership_list[self.ip][0], self.heartbeat_counter, self.membership_list[self.ip][2])
            membership_list = self.membership_list.copy()
            
            for node in nodes_to_send:
                # if(random.sample(self.pml, k=1)[0] == 1):
                self.send_data(pickle.dumps(["MembershipList", membership_list, (self.style.is_set(), self.last_changed_id)]), node)
                
            time.sleep(self.gossip_period)
                 

    def run(self):
        self.start_node()
        parse_args_thread = threading.Thread(target=self.parse_args)
        parse_args_thread.start()
        receive_messages_thread = threading.Thread(target=self.receive_messages)
        receive_messages_thread.start()
        
        # failure_loss_node = threading.Thread(target=self.failure_loss_rate)
        # failure_loss_node.start()
            
        
        send_heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        failure_handling_thread = threading.Thread(target=self.failure_handling)
        
        send_heartbeat_thread.start()
        failure_handling_thread.start()
        
        # failure_loss_node.join()    
        parse_args_thread.join()
        send_heartbeat_thread.join()
        failure_handling_thread.join()
        receive_messages_thread.join()
        
        self.sock.close()            
            
    
    def failure_loss_rate(self):
        
        while(True):
            if(len(self.membership_list.items()) == 10): 
                break
        
        self.pml = [0]*self.val + [1]*(100-self.val)    
        
        while(True):
            current_time = time.time()
            
            while(time.time() - current_time <= 1):
                continue
            
            print(self.membership_list)
            print("Failure list")
            print(self.failure_list)
            
            
            
            
        
        

    '''
    Input: None
    Purpose: Parse the arguments and carry out functions as intended
    Side effects: 1. Sets whether node is the introducer or not
                2. Sets the timeout value
                3. Sets the style
                4. Sets the gossip target
                5. Makes a node leave the group
    Output: None
    '''
    def parse_args(self):
        while(True):
            if(self.halted_complete.is_set()):
                return
            
            input_string = input("Enter argument or type 'help': ")
            if(input_string == "Change Style"):
                self.switch_style()
            elif (input_string == "Membership List"):
                print(self.membership_list)
                print("\n")
            elif (input_string == "ip"):
                print(self.ip)
                print("\n")
            elif "leave" in input_string :
                if(input_string == "leave"):
                    self.halt_node()
                else:
                    ip_address = input_string.split(" ")[0]
                    if ip_address==self.ip:
                        self.halt_node()
                    else:
                        self.send_data(pickle.dumps(["You leave", ]), ip_address)
            elif input_string == "join" :
                if(not(self.is_introducer) and not(self.join_group())):
                    return
            elif("grep" in input_string):
                string_to_grep = input_string.split(" ")[1]
                os.system("grep -c " + string_to_grep + " log/node.log")
            elif input_string == "help":
                print("grep 'String to find'")
                print("Change Style")
                print("Membership List")
                print("leave")
                print("leave 'IP address'")
                print("join")
                print("\n")
            else:
                print("Argument not recognized\n")

                
if __name__ == '__main__':
    logging = logger('node','log/node.log').logger
    open('log/node.log', 'w').close()                  #  For DEBUG purposes
    node = Node()
    node.run()
    




