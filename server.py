import asyncio
import time
import websockets
import json

class Connection:
    def __init__(self, websocket, time_to_live):
        self.ws = websocket
        self.ttd = time_to_live + time.time()

# Set to keep track of all connected clients
connections = set()

def remove_connection_for_socket(websocket):
    for connection in connections:
        if websocket == connection.ws:
            connections.remove(connection)
            return True
    return False

def connected_sockets():
    return [connection.ws for connection in connections] 

async def kill_expired_connections():
    while True:
        for connection in connections:
            if connection.ttd > time.time():
                connections.remove(connection)
        asyncio.sleep(2)

async def server_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register new connection
    connections.add(Connection(websocket, 300))
    try:
        # Keep the connection open and wait for messages (optional, if you only need to send updates)
        async for message in websocket:
            print(f"Received message from client: {message}")
            await websockets.broadcast(connected_sockets(), message)
    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print("A client disconnected")
        remove_connection_for_socket(websocket)
    finally:
        pass

async def time_update_sender():
    """
    Sends a time update to all connected clients periodically.
    """
    while True:
        if connections:
            message = json.dumps({"type": "time_update", "data": "Current time is " + str(asyncio.get_running_loop().time())})
            # Use the broadcast() function to send to all registered connections
            await websockets.broadcast(connected_sockets(), message)
        await asyncio.sleep(2) # Send update every 2 seconds

async def main():
    """
    Main function to start both the server and the update sender task.
    """
    # Start the periodic update task
    kill_connections_task = asyncio.create_task(kill_expired_connections())
    
    # Start the WebSocket server
    async with websockets.serve(server_handler, "localhost", 8765):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    print("WebSocket server started at ws://localhost:8765")
    asyncio.run(main())
