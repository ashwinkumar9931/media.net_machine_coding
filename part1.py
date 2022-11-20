from pickle import FALSE
import socket

FORMAT = 'ascii'


dict = {}

dict_tags = dict()

tag_key_pr = dict()


def retrieve_key(socket, data):

    all_words = data.split()

    if(len(all_words) != 2):
        socket.send("!400\n".encode(FORMAT))
        return

    key = all_words[1]

    if key not in dict.keys():
        socket.send("!404\n".encode(FORMAT))
        return

    msg = "!200 " + dict[key] + "\n"

    socket.send(msg.encode(FORMAT))
    

def list_key_with_tags(socket, data):

    all_words = data.split()

    if(len(all_words) != 3):
        socket.send("!400\n".encode(FORMAT))
        return

    tagname = all_words[2]

    if(tagname not in tag_key_pr.keys() ):
        socket.send("!404\n".encode(FORMAT))
        return

    value = ""


    for i in tag_key_pr[tagname]:
        value = value + i + " "

    socket.send("!200 "+value+"\n".encode(FORMAT))


def get_with_tags(socket, data):

    all_words = data.split()

    if(len(all_words) != 2):
        socket.send("!400\n".encode(FORMAT))
        return

    if all_words[0] not in dict_tags:
        socket.send("!404\n".encode(FORMAT))
        return

    value = dict_tags[all_words[0]]

    socket.send("!200 "+value+"\n".encode(FORMAT))



def key_value_tags(socket, data):
    all_words = data.split()

    key = all_words[1]
    value = all_words[2]

    tags = data.split(' ', 1)[1:][0].split(' ', 1)[1:][0]

    print(tags)

    print(key + "    " + value)

    if key not in dict_tags.keys():
        # pushing tags with corresponding key
        #store tag key pair
        all_words = tags.split()

        print(all_words)

        for i in range(0, len(all_words)):
            dict_tags[key, value] = all_words[i]
            if(i>1):
                if(all_words[i] not in tag_key_pr.keys()):
                    tag_key_pr[all_words[i]] = key
                else:
                    tag_key_pr[all_words[i]].append(key)
                print(all_words[i])

        print(all_words)

        socket.send("!200\n".encode(FORMAT))
        return
    
    for i in dict_tags:
        print(i)

    print("out of loop")
    socket.send("!500\n".encode(FORMAT))
    return



def key_value(socket, data):
    all_words = data.split()
 
    if(len(all_words) < 3):
        socket.send("!400\n".encode(FORMAT))
        return
    
    key = all_words[1]
    value = all_words[2]

    
    if key not in dict.keys():
        dict[key] = value
        socket.send("!200\n".encode(FORMAT))
        return
    socket.send("!500\n".encode(FORMAT))
    
    

def recv_until_eol(s: socket.socket) -> bytearray:
    data = bytearray()
    while True:
        chunk = s.recv(1024)
        data += chunk
        if b'\n' in chunk:
            break
    return data



def Client(socket, address):

    signal = True
    data = ""

    while signal:
        try:

            data = recv_until_eol(socket)

            data = data.decode('ascii')

            print (data)

            res = ""

            if data != "":
                all_words = data.split()
                if(all_words[0] == 'ECHO'):
                    #   hasjdb sajdbj \n
                    res = data.split(' ', 1)[1]
                    res.strip()
                    res = '!200 ' + res
                    socket.send(res.encode(FORMAT))

                if(all_words[0] == 'SET'):
                    if(len(all_words) == 3):
                        key_value(socket, data)
                    else:
                        key_value_tags(socket, data)

                if(all_words[0] == 'GET'):
                    
                    retrieve_key(socket, data)
                if(all_words[0] == 'FLUSH'):
                    dict.clear()
                    socket.send("!200\n".encode(FORMAT))
                if(all_words[0] == 'GETWITHTAGS'):
                    get_with_tags(socket, data)
                if(all_words[0] == 'LISTKEYSWITHTAG'):
                    list_key_with_tags(socket, data)

        except:
            signal = FALSE
    
    socket.close()


def newConnections(socket):
    while True:
        sock, address = socket.accept()
        Client(sock, address)

def main():

    # SERVER = socket.gethostbyname(socket.gethostname())  10.0.0.1
    # PORT = 9999

    host = '127.0.0.1'
    port = 9994
    
    print(host)
    print(port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)
    
    newConnections(socket=sock)
    
main()