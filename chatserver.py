import asyncio
import threading
import time
import websockets
import json

class Connections:
    def __init__(self, connections=None):
        if type(connections) == None:
            self.connections = []
        elif type(connections) == set[tuple[any,int]] \
        or type(connections) == set[Connection] \
        or type(connections) == Connections:
            self.connections = [Connection(connection) for connection in connections]
        elif type(connections) == list:
            self.connections = [Connection(connection) for connection in connections]
        else:
            raise TypeError("Expected Connection or set and got " + str(type(connections)))
        
    def remove_timed_out(self):
        for connection in self.connections:
            if not connection.alive():
                self.connections.remove(connection)

    def add(self, connection):
        self.connections.append(connection)
        return self.connections
    
    def remove(self, connection):
        if isinstance(connection, Connection):
            self.connections = [c for c in self.connections if connection != c]

    def remove_by_websocket(self, websocket):
        self.connections = [c for c in self.connections if c.websocket != websocket] 

    def __iter__(self):
        for connection in self.connections:
            yield connection
    
    def websockets(self):
        for connection in self.connections:
            yield connection.websocket
    
                    
class MessageTimed:
    def __init__(self, message_data, thread_name="Main"):
        message_data["time"] = int(time.time())
        message_data["thread_name"] = thread_name
        self.message_data = message_data
    
    def json(self):
        return json.dumps(self.message_data)
    
    def thread_name(self):
        if 'thread_name' in self.message_data:
            return self.message_data["thread_name"]
        else:
            raise ValueError("Data missing")
    
    def __eq__(self, other):
        if isinstance(other, MessageTimed):
            return self.message_data["time"] == other.message_data["time"]
        else:
            return False
    
    def __gt__(self, other):
        if isinstance(other, MessageTimed):
            return self.message_data["time"] > other.message_data["time"]
        else:
            raise TypeError(f"Cannot compare MessageTimed with {type(other)}")
    
    def __lt__(self, other):
        return not self.__gt__(other) and not self.__eq__(other)

class Connection:
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], Connection):
            self.websocket = args[0].websocket
            self.ttd = args[0].ttd
        elif len(args) == 1 and isinstance(args[0], tuple[any,int]):
            self.websocket, self.ttd = args[0]
        elif len(args) == 2 and isinstance(args[1], int):
            self.websocket = args[0]
            self.ttd = args[1]
        elif len(args) == 2 and isinstance(args[0], int):
            self.websocket = args[1]
            self.ttd = args[0]
        else:
            raise TypeError("Must pass one Connection, or one tuple with ServerConnection and int, or a ServerConnection and an int")

    def tuple(self):
        return (self.websocket, self.ttd)
    
    def alive(self):
        return self.ttd > int(time.time())
        
    def send(self, message : MessageTimed):
        return self.websocket.send(message.json())
        
    def __eq__(self, value):
        if isinstance(value, tuple):
            ws, ttd = value
            return self.websocket == ws and self.ttd == ttd
        elif isinstance(value, Connection):
            return self.websocket == value.websocket and self.ttd == value.ttd
        else:
            return NotImplemented
    
    def __ne__(self,value):
        return not self.__eq__(value)
    

class Thread:
    def __init__(self, name):
        self.name = name
        self.recent_messages : list[MessageTimed] = list()
    
    def __add__(self, message):
        if type(message) == MessageTimed:
            self.recent_messages.append(message)
            return self.recent_messages
        else:
            return NotImplemented
    
    def add(self, message):
        return self.__add__(message)

    def __iter__(self):
        for message in self.recent_messages:
            yield message


class Threads:
    def __init__(self, threads=[], current_thread="Main", connections=[]):
        self.current_thread = current_thread
        self.connections = Connections([Connection(client) for client in connections])

        if isinstance(threads, list):
            self.threads = threads
        elif isinstance(threads, set):
            self.threads = list(threads)
        else:
            raise TypeError("Cannot use" + threads + " which is a " + type(threads) + " as list of threads")

    def __add__(self, other):
        if isinstance(other, Connection):
            self.connections.add(other)
        else:
            try:
                self.connections.add(Connection(other))
            except:
                raise TypeError("Cannot add "+ str(type(other)) +" to Connections")
        return self
    
    def drop_by_connection(self, ws):
        self.connections.remove_by_websocket(ws)
        self.connections = [client for client in self.connections if client.websocket != ws]

    def add_client(self, other):
        return self.__add__(other)

    def broadcast_recent(self, websocket):
        for thread in self.threads:
            if thread.name == self.current_thread:
                for message in thread:
                    websockets.broadcast([websocket], message.json())

    def broadcast_threads(self, websocket):
        message = MessageTimed({"threads" : [thread.name for thread in self.threads]}, thread_name=self.current_thread)
        websockets.broadcast( [websocket], message.json() )
    
    def broadcast(self, message):
        for thread in self.threads:
            if thread.name == message.thread_name():
                thread.add(message)
                websockets.broadcast(self.connections.websockets(), message.json())

    def change_thread(self, new_name):
        self.current_thread = new_name
    
    def __getitem__(self, key, default=None):
        if key == None:
            for thread in self.threads:
                if thread.name == self.current_thread:
                    return thread
        if isinstance(key, str):
            for thread in self.threads:
                if thread.name == key:
                    return thread
        elif isinstance(key, int) and len(self.threads) > key:
                return self.threads[key]
        elif isinstance(key, thread) and thread in self.threads:
                return thread
        else:
            return default

    def __setitem__(self, key, value):
        if isinstance(key, str) and isinstance(value, Thread):
            if key == value.name:
                for i, t in enumerate(self.threads):
                    if t.name == key:
                        self.threads.insert(i, value)
            else:
                raise ValueError("Key doesn't match thread name")
        else:
            return NotImplemented

    def __iter__(self):
        for thread in self.threads:
            yield thread

        

threads = Threads( [Thread("Main"), Thread("Special")], current_thread="Main" )

async def chat_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register client with a time to die (removed from our connection list)
    threads.connections.add(Connection(websocket, int(time.time()) + 300))

    # send most recent 50 messages
    threads.broadcast_recent(websocket)

    # send all the thread names
    threads.broadcast_threads(websocket)

    try:
        # Listen for messages
        async for message in websocket:
            print(f"Received raw message: {message}")

            message_data = None
            try:
                message_data : dict = json.loads(s=message)
            except json.decoder.JSONDecodeError:
                print("message is not well formed json")


            # received a change thread command
            if message_data and "newthread" in message_data.keys():
                print(f"now switching threads from {threads.current_thread} to {message_data['newthread']}")
                threads.current_thread = message_data["newthread"]
                threads.broadcast_recent(websocket)
                threads.broadcast_threads(websocket)

            if message_data and 'username' in message_data.keys() and 'message' in message_data.keys():
                msg_timed = MessageTimed(message_data, thread_name=threads.current_thread)
                threads.broadcast(msg_timed)
                print("sending message " + msg_timed.json())
            
    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected with exception")
        threads[None].drop_by_websocket(websocket)

async def timeout():
    while True:
        threads.connections.remove_timed_out()
        await asyncio.sleep(1)

async def main():
    """
    Starts the WebSocket server.
    """
    kill_connections_task = asyncio.create_task(timeout())

    # Start the server on localhost port 6789
    async with websockets.serve(chat_handler, "127.0.0.1", 6789):
        print("Chat server started on ws://127.0.0.1:6789")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
