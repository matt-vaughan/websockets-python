import asyncio
import websockets
import json

# Set of connected clients
connected_clients = set()

async def chat_handler(websocket):
    """
    Handles a single WebSocket connection.
    """
    # Register client
    connected_clients.add(websocket)
    try:
        # Listen for messages
        async for message in websocket:
            # Prepare message (e.g., add username/timestamp in a real app)
            # For this simple example, we just pass the raw message
            print(f"Received message: {message}")
            
            # Broadcast message to all other connected clients
            other_clients = [client for client in connected_clients if client != websocket]
            if other_clients:
                await asyncio.wait([client.send(message) for client in other_clients])
                
    except websockets.exceptions.ConnectionClosed:
        # Handle disconnection
        print(f"Client disconnected.")
    finally:
        # Unregister client
        connected_clients.remove(websocket)

async def main():
    """
    Starts the WebSocket server.
    """
    # Start the server on localhost port 6789
    async with websockets.serve(chat_handler, "0.0.0.0", 6789):
        print("Chat server started on ws://0.0.0.0:6789")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
