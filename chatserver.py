import asyncio
import time
import websockets
import json

class Connections:
    def __init__(self, connections=[]):
        self.connections = connections
        
    def remove_timed_out(self):
        for connection in self.connections:
            if not connection.alive():
                connection.websocket.close()
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

class Connection:
    def __init__(self, websocket, ttd):
        self.websocket = websocket
        self.ttd = ttd

    def tuple(self):
        return (self.websocket, self.ttd)      

    def alive(self):
        return self.ttd > int(time.time())
        
    def send(self, message):
        return self.websocket.send(message.json())
        
    def __eq__(self, other):
        if isinstance(other, Connection):
            return self.websocket == other.websocket
        elif isinstance(other, websockets.ServerConnection):
            return self.websocket == other
        else:
            return NotImplemented
    
    def __ne__(self,value): return not self == value   
                    
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
        if isinstance(other, Message):
            return self.message_data["time"] == other.message_data["time"]
        else:
            return False
    def __ne__(self,other): return not self == other
    
    def __gt__(self, other):
        if isinstance(other, Message):
            return self.message_data["time"] > other.message_data["time"]
        else:
            return NotImplemented
    def __le__(self, other): return not self > other
    def __lt__(self, other): return not self > other and self != other
    def __ge__(self, other): return not self < other 

class Room:
    MAX = int(50)
    
    def __init__(self, name):
        self.name = name
        self.messages : list[Message] = list()
    
    def __add__(self, message):
        if type(message) == Message:
            self.messages.append(message)
            if Room.MAX < len(self.messages):
                self.messages[len(self.messages)-Room.MAX:]
            return self.messages
        else:
            raise TypeError(message,"expected Message as argument got " + str(type(message)))
    
    def __iadd__(self, other):
        if isinstance(other, Message):
            self + other
            return self
        else:
            return NotImplemented
        
    def add(self, message):
        return self + message

    def __getitem__(self, key):
        match type(key):
            case type(str):
                return [m.message_data[key] for m in self.messages if key in m.message_data]
            case type(int):
                return self.messages[key]
            case _:
                return NotImplemented
            
    def __iter__(self):
        for message in self.messages:
            yield message


class Rooms:

    rooms = list()
    connections = Connections()
    current_room : str 

    instance = None

    @classmethod
    def __class_getitem__(cls, key=None):
        if not cls.instance:
            cls.instance = Rooms( rooms=[Room("Main"), Room("Special") ], current_room="Main" )
        if not key:
            return cls.instance
        elif type(key) == str:
            return cls.instance[key]

    def __init__(self, rooms=[], current_room="Main", connections=[]):
        Rooms.current_room = current_room
        for connection in [Connection(client) for client in connections]:
            Rooms.connections.add(connection)

        if isinstance(rooms, list):
            Rooms.rooms.extend(rooms)
        elif isinstance(rooms, set):
            Rooms.rooms.extend(list(rooms))
        else:
            raise TypeError("Cannot use" + rooms + " which is a " + type(rooms) + " as list of rooms")

    def __add__(self, other):
        if isinstance(other, Connection):
            Rooms.connections.add(other)
        elif isinstance(other, list) or isinstance(other, dict) or isinstance(other, tuple):
            Rooms.connections.add(Connection(other))
        else:    
            raise TypeError("Cannot add "+ str(type(other)) +" to Connections")
        return Rooms.instance
    
    def drop_by_connection(self, ws):
        Rooms.connections.remove_by_websocket(ws)
        Rooms.connections = [client for client in Rooms.connections if client.websocket != ws]

    def add_client(self, other):
        return self.__add__(other)

    def broadcast_recent(self, websocket):
        for room in Rooms.rooms:
            if room.name == Rooms.current_room:
                for message in room:
                    websockets.broadcast([websocket], message.json())

    def broadcast_rooms(self, websocket):
        message = Message({"rooms" : [room.name for room in Rooms.rooms]}, room_name=Rooms.current_room)
        websockets.broadcast( [websocket], message.json() )

    def broadcast_rooms_all(self):
        message = Message({"rooms" : [room.name for room in Rooms.rooms]}, room_name=Rooms.current_room)
        websockets.broadcast(Rooms.connections.websockets(), message.json())
    
    def broadcast(self, message):
        for room in Rooms.rooms:
            if room.name == message.room_name():
                room += message
                websockets.broadcast(Rooms.connections.websockets(), message.json())

    def change_room(self, new_name):
        Rooms.current_room = new_name

    def new_room(self, name):
        room = Room(name)
        Rooms.rooms.append( room )
        return room
    
    def __getitem__(self, key):
        if key == None:
            for room in Rooms.rooms:
                if room.name == Rooms.current_room:
                    return room
        if isinstance(key, str):
            for room in Rooms.rooms:
                if room.name == key:
                    return room
        elif isinstance(key, int) and len(Rooms.rooms) > key:
                return Rooms.rooms[key]
        elif isinstance(key, room) and room in Rooms.rooms:
                return room
        else:
            return Rooms     

    def __iter__(self):
        for room in Rooms.rooms:
            yield room

class Users:
    Users : dict[str,dict] = dict()

    @classmethod
    def new_user(cls, name, room, websocket) -> dict:
          cls.Users[name] = dict( {'name': name, 'time' : int(time.time()), 'room' : room, 'websocket' : websocket } )
          return cls.Users[name]

    @classmethod
    def update_name(cls, old, new):
        user = cls.Users.pop(old)
        user['name'] = new
        cls.Users[new] = user

    @classmethod
    def user_active(cls, name):
        cls.Users[name]['time'] = int(time.time())

    @classmethod
    def change_room(cls, name, new_room):
        cls.Users[name]['room'] = new_room
        cls.user_active(name)
    
    @classmethod
    def matching_websocket(cls, websocket):
        for name, user in cls.Users.items():
            if user['websocket'] == websocket:
                return name
        return None
    
    @classmethod
    def broadcast(cls, message: Message):
        for user in cls.Users.values():
            if message.room_name() == user['room']:
                websockets.broadcast( [user['websocket']], message.json())
    
    @classmethod
    def __class_getitem__(cls, name):
        return cls.Users[name] if name in cls.Users.keys() else None


async def chat_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register client with a time to die (removed from our connection list)
    Rooms[None]
    rooms = Rooms.instance

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
            if message_data and 'change_room' in message_data.keys() and 'username' in message_data.keys():
                print(f"now switching rooms to {message_data['change_room']}")
                
                Users.change_room(message_data['username'], message_data["change_room"])
                
                rooms.change_room(message_data['change_room']) 
                rooms.broadcast_recent(websocket)
                rooms.broadcast_rooms(websocket)

            elif message_data and 'username' in message_data.keys() and 'message' in message_data.keys():
                user = Users[message_data['username']]
                # register new user if this is the first time we see him
                if not user:
                    # check for changed username
                    old_username = Users.matching_websocket(websocket)
                    if old_username:
                        Users.update_name(old_username, message_data["username"])
                        print(f"updated username from {old_username} to {message_data['username']}")
                    else:
                        user = Users.new_user(message_data["username"], rooms.current_room, websocket)
                        print("created user " + str(user))
                else:
                    Users.user_active(message_data['username'])
                    print("marked user "+message_data["username"]+" active")
                
                # broadcast message
                m = Message(message_data, room_name=Rooms.current_room)
                Users.broadcast(m)
                print("sending message " + m.json())
            
            elif message_data and 'newroom' in message_data.key():
                rooms.new_room(message_data['newroom'])
                print("created the new room " + message_data['newroom'])
                rooms.broadcast_rooms_all()

                

    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected with exception")
        rooms.drop_by_connection(websocket)

async def timeout():
    while True:
        rooms = Rooms[None]
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
