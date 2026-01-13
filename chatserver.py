import asyncio
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
    
                    
class Message:
    def __init__(self, message_data:dict, room_name="Main"):
        message_data["time"] = int(time.time())
        message_data["room_name"] = room_name
        self.message_data = message_data
    
    def __iter__(self):
        for key, value in self.message_data.items():
            yield key, value

    def json(self):
        return json.dumps(self.message_data)
    
    def room_name(self):
        if 'room_name' in self.message_data:
            return self.message_data["room_name"]
        else:
            raise ValueError("Data missing")
    
    def __eq__(self, other):
        if isinstance(other, Message
    ):
            return self.message_data["time"] == other.message_data["time"]
        else:
            return False
    
    def __gt__(self, other):
        if isinstance(other, Message
    ):
            return self.message_data["time"] > other.message_data["time"]
        else:
            raise TypeError(f"Cannot compare Message with {type(other)}")
    
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
        
    def send(self, message : Message):
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
    

class Room:
    MAX = int(50)
    
    def __init__(self, name):
        self.name = name
        self.recent_messages : list[Message] = list()
    
    def __add__(self, message):
        if type(message) == Message:
            self.recent_messages.append(message)
            if Room.MAX < len(self.recent_messages):
                self.recent_messages[len(self.recent_messages)-Room.MAX:]
            return self.recent_messages
        else:
            raise TypeError(message,"expected Message as argument got " + str(type(message)))
    
    def add(self, message):
        return self.__add__(message)

    def __iter__(self):
        for message in self.recent_messages:
            yield message


class Rooms:
    def __init__(self, rooms=[], current_room="Main", connections=[]):
        self.current_room = current_room
        self.connections = Connections([Connection(client) for client in connections])

        if isinstance(rooms, list):
            self.rooms = rooms
        elif isinstance(rooms, set):
            self.rooms = list(rooms)
        else:
            raise TypeError("Cannot use" + rooms + " which is a " + type(rooms) + " as list of rooms")

    def __add__(self, other):
        if isinstance(other, Connection):
            self.connections.add(other)
        elif isinstance(other, list) or isinstance(other, dict) or isinstance(other, tuple):
            self.connections.add(Connection(other))
        else:    
            raise TypeError("Cannot add "+ str(type(other)) +" to Connections")
        return self
    
    def drop_by_connection(self, ws):
        self.connections.remove_by_websocket(ws)
        self.connections = [client for client in self.connections if client.websocket != ws]

    def add_client(self, other):
        return self.__add__(other)

    def broadcast_recent(self, websocket):
        for room in self.rooms:
            if room.name == self.current_room:
                for message in room:
                    websockets.broadcast([websocket], message.json())

    def broadcast_rooms(self, websocket):
        message = Message({"rooms" : [room.name for room in self.rooms]}, room_name=self.current_room)
        websockets.broadcast( [websocket], message.json() )
    
    def broadcast(self, message):
        for room in self.rooms:
            if room.name == message.room_name():
                room += message
                websockets.broadcast(self.connections.websockets(), message.json())

    def change_room(self, new_name):
        self.current_room = new_name
    
    def __getitem__(self, key, default=None):
        if key == None:
            for room in self.rooms:
                if room.name == self.current_room:
                    return room
        if isinstance(key, str):
            for room in self.rooms:
                if room.name == key:
                    return room
        elif isinstance(key, int) and len(self.rooms) > key:
                return self.rooms[key]
        elif isinstance(key, room) and room in self.rooms:
                return room
        else:
            return default     
        
    def __setitem__(self, key, value):
        if isinstance(key, str) and isinstance(value, Room):
            if key == value.name:
                for i, t in enumerate(self.rooms):
                    if t.name == key:
                        self.rooms.insert(i, value)
            else:
                raise ValueError("Key doesn't match room name")
        else:
            return NotImplemented

    def __iter__(self):
        for room in self.rooms:
            yield room

        

rooms = Rooms( [Room("Main"), Room("Special") ], current_room="Main" )

async def chat_handler(websocket : websockets.ServerConnection):
    """
    Handles a single WebSocket connection.
    """
    # Register client with a time to die (removed from our connection list)
    rooms.connections.add(Connection(websocket, int(time.time()) + 300))

    # send most recent 50 messages
    rooms.broadcast_recent(websocket)

    # send all the room names
    rooms.broadcast_rooms(websocket)

    try:
        # Listen for messages
        async for message in websocket:
            print(f"Received raw message: {message}")

            message_data = None
            try:
                message_data : dict = json.loads(s=message)
            except json.decoder.JSONDecodeError:
                print("message is not well formed json")


            # received a change room command
            if message_data and "newroom" in message_data.keys():
                print(f"now switching rooms from {rooms.current_room} to {message_data['newroom']}")
                rooms.current_room = message_data["newroom"]
                rooms.broadcast_recent(websocket)
                rooms.broadcast_rooms(websocket)

            if message_data and 'username' in message_data.keys() and 'message' in message_data.keys():
                m = Message(message_data, room_name=rooms.current_room)
                rooms.broadcast(m)
                print("sending message " + m.json())

    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected with exception")
        rooms.drop_by_connection(websocket)

async def timeout():
    while True:
        rooms.connections.remove_timed_out()
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
