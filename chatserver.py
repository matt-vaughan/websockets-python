import asyncio
import threading
import time
import websockets
import json

# Set of connected clients
connected_clients = set()

# last 50 messages
recent_messages = list()

class MessageTimed:
    def __init__(self, message, username="Unknown", thread=connected_clients):
        self.message = message
        self.time = time.time()
        self.username = username
        self.thread = thread
    
    def jsonString(self):
        d = dict()
        d["username"] = self.username
        d["time"] = self.time
        d["message"] = self.message
        return json.dumps(d)

def add_message(message, username="unknown"):
    global recent_messages
    msgTimed = MessageTimed(message, username)
    recent_messages.append(msgTimed)
    if len(recent_messages) > 50:
        recent_messages = recent_messages[len(recent_messages)-50:len(recent_messages)]
    return msgTimed

def remove_by_websocket(websocket, client=connected_clients):
    for ws, ttd in connected_clients:
        if ws == websocket and (ws,ttd) in connected_clients:
            connected_clients.remove((ws,ttd))

async def chat_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register client
    connected_clients.add((websocket, time.time() + 300))

    # send most recent 50 messages
    if recent_messages:
        [await websocket.send(timedmsg.message) for timedmsg in recent_messages]
    try:
        # Listen for messages
        async for message in websocket:
            # Prepare message (e.g., add username/timestamp in a real app)
            # For this simple example, we just pass the raw message
            print(f"Received message: {message}")

            message_data = None
            try:
                message_data : dict = json.loads(s=message)
            except json.decoder.JSONDecodeError:
                print("message is not well formed json")

            # save message to list of 50 recent messages and broadcast to all users
            if message_data and message_data["username"] and message_data["message"]:
                timed_msg = add_message(message_data["message"], username=message_data["username"])
                
                # Broadcast message to all clients including self
                websockets.broadcast([ws for ws, ttd in connected_clients], timed_msg.jsonString())
            else:
                # ssave unidentified message
                add_message(message)
            
            
            # Broadcast message to all other connected clients
            #other_clients = [client for client in connected_clients if client != websocket]
            #if other_clients:
            #    await asyncio.wait([client.send(message) for client in other_clients])
    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected with exception")
        remove_by_websocket(websocket)

async def timeout():
    while True:
        for ws, ttd in connected_clients:
            if ttd < time.time():
                connected_clients.remove((ws,ttd))
                print(f"removed websocket client who timed out with ttd {ttd}")
                break
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
