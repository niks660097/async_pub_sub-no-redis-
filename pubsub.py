import socket
import sys
import threading
import cPickle
import time
import datetime
from collections import deque

HOST = '127.0.0.1'
PORT = 5555#only ganja boys will understand..

class DataStore(object):

    def __init__(self):
        self.data = {}

    def add_message(self, channel, message):
        if self.data.get(channel):
            self.data.(message,)

    def read_message(self, channel):
        try:
            return self.data[channel]
        except:
            return 'null'

def handle_query(conn, data_store):
     data = conn.recv(1024)
     if data:
        try:
            proto_data = cPickle.loads(data)
            if proto_data['sender'] == 'client':
                name = proto_data['channel']
                message = data_store.read_message(name)
                conn.send(message)
            else:
                name = proto_data['channel']
                data_store.add_message(name, proto_data['message'])
        except:
            print 'Invalid data'
     conn.close()

def pub_sub_pipe(host, port):
        data_store = DataStore()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind((HOST, PORT))
        except socket.error as msg:
            print 'Bind failed pub. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            sys.exit()
        s.listen(10)
        #now keep talking with the client
        while 1:#blocking
             conn, addr = s.accept()
             threading.Thread(target=handle_query, args=(conn,data_store)).start()
        s.close()
#starting pub server


class Publisher(object):
    LOCAL_PUB_REG = {}

    def __init__(self, channel='default'):
        self.message = ''
        Publisher.LOCAL_PUB_REG[channel] = self

    def broadcast(self, message):
        self.message = message


class Subscriber(object):

    def __init__(self, channel_name):
        self.channel_name = channel_name
        self.last_message = ''

    def open_connection(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((HOST, PORT))
        except socket.error as msg:
            print 'Bind failed sub. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        return s

    def recv(self):
        soc = self.open_connection()
        soc.sendall(cPickle.dumps({'channel': self.channel_name}))
        message =  soc.recv(1024)
        if message == self.last_message:
            return 'no new data'
        else:
            self.last_message = message
            return message

threading.Thread(target=pub_sub_pipe, args=('', PORT)).start()
