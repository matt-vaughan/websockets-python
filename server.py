import asyncio
import websockets
import json

# Set to keep track of all connected clients
CONNECTED = set()

async def server_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register new connection
    CONNECTED.add(websocket)
    try:
        # Keep the connection open and wait for messages (optional, if you only need to send updates)
        async for message in websocket:
            print(f"Received message from client: {message}")
            # You can process client messages here
            pass
    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print("A client disconnected")
    finally:
        # Unregister connection
        CONNECTED.remove(websocket)

async def time_update_sender():
    """
    Sends a time update to all connected clients periodically.
    """
    while True:
        if CONNECTED:
            message = json.dumps({"type": "time_update", "data": "Current time is " + str(asyncio.get_running_loop().time())})
            # Use the broadcast() function to send to all registered connections
            await websockets.broadcast(CONNECTED, message)
        await asyncio.sleep(2) # Send update every 2 seconds

async def main():
    """
    Main function to start both the server and the update sender task.
    """
    # Start the periodic update task
    update_task = asyncio.create_task(time_update_sender())
    
    # Start the WebSocket server
    async with websockets.serve(server_handler, "localhost", 8765):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    print("WebSocket server started at ws://localhost:8765")
    asyncio.run(main())
