import asyncio
import json
import logging
from typing import Dict, Any, Optional, Callable
import websockets
import base64

from .client import BackpackConfig


logger = logging.getLogger(__name__)


class BackpackWebSocket:
    def __init__(self, config: BackpackConfig):
        self.config = config
        self.ws_url = "wss://ws.backpack.exchange"
        self.websocket = None
        self.subscriptions = set()
        self.handlers = {}
        self.running = False

    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.ws_url)
            self.running = True
            logger.info("Connected to Backpack WebSocket")
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            raise

    async def disconnect(self):
        self.running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("Disconnected from Backpack WebSocket")

    async def subscribe_public(self, streams: list[str]):
        if not self.websocket:
            raise RuntimeError("WebSocket not connected")
        
        message = {
            "method": "SUBSCRIBE",
            "params": streams
        }
        
        await self.websocket.send(json.dumps(message))
        self.subscriptions.update(streams)
        logger.info(f"Subscribed to public streams: {streams}")

    async def subscribe_private(self, streams: list[str]):
        if not self.websocket:
            raise RuntimeError("WebSocket not connected")
        
        # Generate signature for private stream subscription
        import time
        import nacl.signing
        import nacl.encoding
        
        timestamp = int(time.time() * 1000)
        signing_string = f"instruction=subscribe&timestamp={timestamp}&window={self.config.window_ms}"
        
        private_key = nacl.signing.SigningKey(
            self.config.private_key, encoder=nacl.encoding.Base64Encoder
        )
        verify_key = private_key.verify_key
        
        signature = private_key.sign(
            signing_string.encode(), encoder=nacl.encoding.Base64Encoder
        )
        
        message = {
            "method": "SUBSCRIBE",
            "params": streams,
            "signature": [
                base64.b64encode(verify_key.encode()).decode(),
                signature.signature.decode(),
                str(timestamp),
                str(self.config.window_ms)
            ]
        }
        
        await self.websocket.send(json.dumps(message))
        self.subscriptions.update(streams)
        logger.info(f"Subscribed to private streams: {streams}")

    async def unsubscribe(self, streams: list[str]):
        if not self.websocket:
            raise RuntimeError("WebSocket not connected")
        
        message = {
            "method": "UNSUBSCRIBE",
            "params": streams
        }
        
        await self.websocket.send(json.dumps(message))
        self.subscriptions.difference_update(streams)
        logger.info(f"Unsubscribed from streams: {streams}")

    def add_handler(self, stream: str, handler: Callable[[Dict[str, Any]], None]):
        self.handlers[stream] = handler

    async def listen(self):
        if not self.websocket:
            raise RuntimeError("WebSocket not connected")
        
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle subscription responses
                    if "result" in data:
                        continue
                    
                    # Handle stream data
                    if "stream" in data and "data" in data:
                        stream = data["stream"]
                        payload = data["data"]
                        
                        if stream in self.handlers:
                            try:
                                await self.handlers[stream](payload)
                            except Exception as e:
                                logger.error(f"Error in handler for {stream}: {e}")
                        else:
                            logger.debug(f"No handler for stream: {stream}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode WebSocket message: {e}")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.running = False
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            self.running = False

    async def start(self):
        await self.connect()
        
        # Start listening in background
        asyncio.create_task(self.listen())
        
        # Send ping every 60 seconds to keep connection alive
        asyncio.create_task(self._ping_loop())

    async def _ping_loop(self):
        while self.running:
            try:
                if self.websocket:
                    await self.websocket.ping()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Ping failed: {e}")
                break

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()