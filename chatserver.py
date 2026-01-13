import asyncio
import threading
import time
import websockets
import json


# Set of connected clients
connected_clients = set()

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
    try:
        # Listen for messages
        async for message in websocket:
            # Prepare message (e.g., add username/timestamp in a real app)
            # For this simple example, we just pass the raw message
            print(f"Received message: {message}")
            
            # Broadcast message to all clients including self
            websockets.broadcast([ws for ws, ttd in connected_clients], message)
            
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
