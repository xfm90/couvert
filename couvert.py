import base64
import mutex
import socket
import threading
import sqlite3
import time
import sys

lock = mutex.mutex()
buffered_data = {}


bufferdbfile='buffer.db'

socket_to_buffer_table='socket_to_buffer'
buffer_to_socket_table='buffer_to_socket'


def write_to_buffer(bufferdb, table, connid, data):
    with bufferdb:
        cur = bufferdb.cursor()
        cur.execute('INSERT INTO %s (connid, data) VALUES (?,?)' % table, (connid, data))

def socket_reader_thread(conn_id, table, s):
    global buffered_data, lock

    bufferdb = sqlite3.connect(bufferdbfile, 20000)

    #print "socket_reader_thread (ID=%d, table=%s)" % (conn_id, table)

    while True:
        if (buffered_data.has_key(conn_id)):
            try:
                data = s.recv(768)
            except socket.error, (errno, strerror):
                #print "[*] (ID = %d): %d %s" % (conn_id, errno, strerror)

                while (lock.testandset() == False):
                    pass

                # write to buffer
                # write_fp.write("%d #DISCONNECT#$" % (conn_id))
                #print "writing to buffer (%d #DISCONNECT#), table=%s" % (conn_id, table)
                write_to_buffer(bufferdb, table, conn_id, '#DISCONNECT#')

                del buffered_data[conn_id]

                s.close()

                lock.unlock()
                break
            while (lock.testandset() == False):
                pass

            if (data != ''):

                encoded_data = base64.b64encode(data)

                # write to buffer
                # __.write("%d %s$" % (conn_id, encoded_data))
                #print "writing to buffer (%d %s$)" % (conn_id, encoded_data)
                write_to_buffer(bufferdb, table, conn_id, encoded_data)

                # debug
                print "data read from socket (%d)" % len(encoded_data)
                #print "data read from socket (%s)" % encoded_data

            lock.unlock()
        else:
            s.close()
            break

def socket_writer_thread(conn_id, table, s):
    global buffered_data, lock


    #print "socket_writer_thread (ID=%d, table=%s)" % (conn_id, table)

    while True:
        if (buffered_data.has_key(conn_id)):
            if (len(buffered_data[conn_id]) > 0):
                while (lock.testandset() == False):
                    pass

                try:
                    data = buffered_data[conn_id].pop(0)
                except KeyError, (errno):
                    lock.unlock()
                    break

                lock.unlock()

                s.send(data)

                # debug
                #print "data read from socket encoded len (%d)" % len(data)
                #print "data read from socket (%s)" % data
            else:
                time.sleep(0.001)
        else:
            break

        def socket_writer_thread(conn_id, table, s):
            global buffered_data, lock

            #print "socket_writer_thread (ID=%d, table=%s)" % (conn_id, table)

            while True:
                if (buffered_data.has_key(conn_id)):
                    if (len(buffered_data[conn_id]) > 0):
                        while (lock.testandset() == False):
                            pass

                        try:
                            data = buffered_data[conn_id].pop(0)
                        except KeyError, (errno):
                            lock.unlock()
                            break

                        lock.unlock()

                        s.send(data)

                        # debug
                        #print "data read from socket encoded len (%d)" % len(data)
                        #print "data read from socket (%s)" % data
                    else:
                        time.sleep(0.001)
                else:
                    break




# loop listening for new socket connections
def connection_accepter():
    global lock, bufferdb

    bufferdb = sqlite3.connect(bufferdbfile, 20000)

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((address, port))
    next_conn_id = 1

    while True:
        serversocket.listen(5)
        (s, clientaddress) = serversocket.accept()
        conn_id = next_conn_id
        next_conn_id += 1
        #print "[*] Connection received (ID = %d) from %s:%d" % (conn_id, clientaddress[0], clientaddress[1])

        while(lock.testandset() == False):
            pass

        # write to buffer
        # _.write("%d #CONNECT#$" % conn_id)
        #print "writing to buffer (%d #CONNECT#)" % (conn_id)
        write_to_buffer(bufferdb, socket_to_buffer_table, conn_id, '#CONNECT#')

        buffered_data[conn_id] = []

        lock.unlock()

        t = threading.Thread(target=socket_reader_thread, name="t%d" % conn_id, args=[conn_id, socket_to_buffer_table, s])
        t.start()
        t = threading.Thread(target=socket_writer_thread, name="t%d" % conn_id, args=[conn_id, buffer_to_socket_table, s])
        t.start()









# read from db buffer and process connections IDs + data
def buffered_reader(table):

    #print 'in buffered reader'

    bufferdb = sqlite3.connect(bufferdbfile, 20000)


    while True:

        cd = None
        with bufferdb:
            cur = bufferdb.cursor()
            cur.execute('SELECT id, connid, data from %s ORDER BY id ASC' % table)
            cd = cur.fetchall()


            if cd is not None and len(cd)>0:
                for id, connid, data in cd:
                    #print connid, data
                    # do stuff wit it
                    process_packet(connid, data)

                    # then delete because it was processed (@TODO rollback if nok!)
                    cur.execute('DELETE FROM %s WHERE id=?' % table, (id,))

            else:
                time.sleep(0.1) # it was 0.001


# Process file packet
def process_packet(connid, data):
    global lock


    conn_id = int(connid)

    #print "in process_packet (ID=%d, data=%s)" % (conn_id, data)

    while (lock.testandset() == False):
        pass

    if (data == "#CONNECT#"):
        print "[*] Connection request received (ID=%d). Connecting to %s on port %d" % (conn_id, address, port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((address, port))

        buffered_data[conn_id] = []
        t = threading.Thread(target=socket_reader_thread, name="r%d" % conn_id, args=[conn_id, buffer_to_socket_table, s])
        t.start()

        t = threading.Thread(target=socket_writer_thread, name="w%d" % conn_id, args=[conn_id, socket_to_buffer_table, s])
        t.start()

    elif (data == "#DISCONNECT#"):
        print "[*] Disconnect request received (ID=%d). Connection terminated " % (conn_id)
        del buffered_data[conn_id]
    else:
        #print "[*] plain data (ID=%d, %s)" % (conn_id, data)
        decoded_data = base64.b64decode(data)
        try:
            buffered_data[conn_id].append(decoded_data)
        except KeyError, (errno):
            pass

        # debug
        print "data written to socket (%d)" % len(data)
        #print "data written to socket (%s)" % data

    lock.unlock()




def usage():
    print "Usage: %s <mode> <ip_address> <tcp_port>\n" % sys.argv[0]
    print "\tmode = client - connect to ip:tcp"
    print "\tmode = server - listens on ip:tcp"




if __name__ == '__main__':

    if (len(sys.argv) != 4):
        usage()
        sys.exit(1)

    mode = sys.argv[1]
    address = sys.argv[2]
    port = int(sys.argv[3])

    bufferdb = sqlite3.connect(bufferdbfile, 20000)

    #with bufferdb:
    #    cur = bufferdb.cursor()
    #    cur.execute('SELECT SQLITE_VERSION()')

    #    data = cur.fetchone()

        #print "SQLite version: %s" % data

        #cur.execute("DROP TABLE IF EXISTS %s" % socket_to_buffer_table)
        #cur.execute("CREATE TABLE %s(id INTEGER PRIMARY KEY AUTOINCREMENT, connid INTEGER, data TEXT)" % socket_to_buffer_table)

        #cur.execute("DROP TABLE IF EXISTS %s" % buffer_to_socket_table)
        #cur.execute("CREATE TABLE %s(id INTEGER PRIMARY KEY AUTOINCREMENT, connid INTEGER, data TEXT)" % buffer_to_socket_table)


    if mode == 'server':
        print 'server mode'
        t = threading.Thread(target = connection_accepter, name="connection accepter", args=[])
        t.start()
        t = threading.Thread(target = buffered_reader, name="buffered reader", args=[buffer_to_socket_table])
        t.start()
    elif mode == 'client':
        print 'client mode'
        t = threading.Thread(target = buffered_reader, name="buffered reader", args=[socket_to_buffer_table])
        t.start()
    else:
        print 'unkown mode\n'
        usage()
        sys.exit(1)



    while True:
        time.sleep(0.1)