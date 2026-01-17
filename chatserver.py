import asyncio
import time
import websockets
import json
                    
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
        
    def add(self, message):
        self.messages.append(message)
        return self.messages

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
    _instance = None

    # this is a singleton class
    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = Rooms( [Room("Main"), Room("Special")] )
        return cls._instance

    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
                
    def __init__(self, rooms=[]):
        self.rooms = rooms
        
    def broadcast_recent(self, websocket, room_name):
        for room in self.rooms: 
            for message in room:
                if room.name == room_name:
                    websockets.broadcast( [websocket], message.json() )
    
    def broadcast_users(self, websocket, room_name):
        users = [name for name, user in Users.Users.items() if user['room'] == room_name ]
        message = Message({"users" : users }, "*")
        websockets.broadcast( [websocket], message.json() )

    def broadcast_rooms(self, websocket, room_name):
        message = Message({"rooms" : [room.name for room in self.rooms]}, room_name)
        websockets.broadcast( [websocket], message.json() )

    def broadcast_rooms_all(self):
        message = Message({"rooms" : [room.name for room in self.rooms]}, room_name="*")
        targets = [ user['websocket'] for user in Users.Users.values() ]
        websockets.broadcast( targets, message.json())

    def new_room(self, name):
        room = Room(name)
        self.rooms.append( room )
        return room

    def __getitem__(self, key):
        for room in self.rooms:
            if room.name == key:
                return room
        return None
            
    def __iter__(self):
        for room in self.rooms:
            yield room

class Users:
    Users : dict[str,dict] = dict()

    @classmethod
    def new_user(cls, name, room, websocket) -> dict:
          cls.Users[name] = dict( {'name': name, 'time' : int(time.time()), 'room' : room, 'websocket' : websocket } )
          return cls.Users[name]

    @classmethod
    def login(cls, username, websocket):
        # update websocket or username depending
        if username in cls.Users.keys():
            cls.Users[username]['websocket'] = websocket
            cls.Users[username]['time'] = int(time.time())
        else:
            # check for changed username
            old_username = Users.matching_websocket(websocket)
            if old_username:
                Users.update_name(old_username, username)
            else:
                # create a user if one doens't exist
                user = Users.new_user(username, "Main", websocket)
        return cls.Users[username]
            
    @classmethod
    def update_name(cls, old, new):
        user = cls.Users.pop(old)
        user['name'] = new
        cls.Users[new] = user
        return user

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
        rooms = Rooms.instance()
        room = rooms[message.room_name()]
        if rooms is not None: 
            room.add(message) 
        for user in cls.Users.values():
            if message.room_name() == user['room'] or message.room_name() == "*":
                websockets.broadcast( [user['websocket']], message.json())
    
    @classmethod
    def disconnect(cls, name):
        user = cls.Users[name]
        #user['websocket'].close()
        del cls.Users[name]
        return user

    @classmethod
    def remove_timed_out(cls, timeout):
        timed_out_users = [name for name, user in cls.Users.items() if user['time'] + timeout < int(time.time())]
        [Users.disconnect(name) for name in timed_out_users]
        return timed_out_users

    @classmethod
    def __class_getitem__(cls, name):
        return cls.Users[name] if name in cls.Users.keys() else None


async def chat_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register client with a time to die (removed from our connection list)
    rooms = Rooms.instance()

    try:
        # Listen for messages
        async for message in websocket:
            print(f"Received raw message: {message}")

            message_data = None
            try:
                message_data : dict = json.loads(s=message)
            except json.decoder.JSONDecodeError:
                print("message is not well formed json")

            # initial login
            if message_data and 'login' in message_data.keys():
                # login user
                user = Users.login(message_data['username'], websocket)

                # send most recent 50 messages
                rooms.broadcast_recent(websocket, user['room'])

                # send all the room names
                rooms.broadcast_rooms(websocket, user['room'])

                # send list of all users in this chat room
                rooms.broadcast_users(websocket, user['room'])
            
            # received a change room command
            elif message_data and 'change_room' in message_data.keys() and 'username' in message_data.keys():
                print(f"now switching rooms to {message_data['change_room']}")
                
                Users.change_room(message_data['username'], message_data["change_room"])
                
                rooms.broadcast_recent(websocket, message_data['change_room'])
                rooms.broadcast_rooms(websocket, message_data['change_room'])
                rooms.broadcast_users(websocket, message_data['change_room'])

            elif message_data and 'new_room' in message_data.keys():
                rooms.new_room(message_data['new_room'])
                print("created the new room " + message_data['new_room'])
                rooms.broadcast_rooms_all()

            elif message_data and 'direct_message' in message_data.keys() and 'username' in message_data.keys() and 'message' in message_data.keys():
                # repeat login to update websocket and username and mark user active
                user = Users.login(message_data['username'], websocket)
                
                if Users[message_data['username']]:
                    m = Message(message_data, "*dm*");
                    target_user = Users[message_data['username']]
                    websockets.broadcast( [target_user['websocket']], m.json() );
                    print("sending message " + m.json())

            elif message_data and 'username' in message_data.keys() and 'message' in message_data.keys():
                # repeat login to update websocket and username and mark user active
                user = Users.login(message_data['username'], websocket)
                
                # broadcast message
                m = Message(message_data, user['room'])
                Users.broadcast(m)
                print("sending message " + m.json())            

    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected with exception")
        user = Users.matching_websocket(websocket)
        Users.disconnect(user)

async def timeout():
    while True:
        timed_out_users = Users.remove_timed_out( 600 ) # 10 minutes for timeout
        if timed_out_users != []:
            print("Client(s) timed out: " + str(timed_out_users))
        await asyncio.sleep(2)

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
