import base64
import time
from typing import Optional, Dict, Any, List
import json
import asyncio
from urllib.parse import urlencode
import logging

import httpx
import nacl.signing
import nacl.encoding
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BackpackConfig(BaseModel):
    api_key: str
    private_key: str
    base_url: str = "https://api.backpack.exchange"
    window_ms: int = 5000


class BackpackClient:
    def __init__(self, config: BackpackConfig):
        self.config = config
        self.private_key = nacl.signing.SigningKey(
            config.private_key, encoder=nacl.encoding.Base64Encoder
        )
        self.verify_key = self.private_key.verify_key
        self.client = httpx.AsyncClient(base_url=config.base_url)

    def _generate_signature(self, instruction: str, params: Dict[str, Any]) -> str:
        timestamp = int(time.time() * 1000)
        
        # Sort parameters alphabetically and create query string
        sorted_params = sorted(params.items())
        param_string = urlencode(sorted_params)
        
        # Create signing string - only add param_string if it's not empty
        if param_string:
            signing_string = f"instruction={instruction}&{param_string}&timestamp={timestamp}&window={self.config.window_ms}"
        else:
            signing_string = f"instruction={instruction}&timestamp={timestamp}&window={self.config.window_ms}"
        
        logger.debug(f"Signing string: {signing_string}")
        
        # Sign the string
        signature = self.private_key.sign(
            signing_string.encode(), encoder=nacl.encoding.Base64Encoder
        )
        
        sig_data = {
            "signature": signature.signature.decode(),
            "timestamp": str(timestamp),
            "window": str(self.config.window_ms)
        }
        logger.debug(f"Generated signature data: {sig_data}")
        
        return sig_data

    def _get_headers(self, instruction: str, params: Dict[str, Any]) -> Dict[str, str]:
        sig_data = self._generate_signature(instruction, params)
        api_key = self.verify_key.encode(encoder=nacl.encoding.Base64Encoder).decode()
        
        headers = {
            "X-API-Key": api_key,
            "X-Signature": sig_data["signature"],
            "X-Timestamp": sig_data["timestamp"],
            "X-Window": sig_data["window"],
            "Content-Type": "application/json"
        }
        
        logger.debug(f"Generated headers for {instruction}: {headers}")
        return headers

    async def get_balances(self) -> Dict[str, Any]:
        headers = self._get_headers("balanceQuery", {})
        logger.debug(f"Balance request headers: {headers}")
        logger.debug(f"Balance request URL: {self.client.base_url}/api/v1/capital")
        
        response = await self.client.get("/api/v1/capital", headers=headers)
        logger.debug(f"Balance response status: {response.status_code}")
        logger.debug(f"Balance response headers: {response.headers}")
        
        if response.status_code != 200:
            logger.error(f"Balance request failed with status {response.status_code}")
            logger.error(f"Response body: {response.text}")
            
        response.raise_for_status()
        return response.json()

    async def get_collateral(self) -> Dict[str, Any]:
        headers = self._get_headers("collateralQuery", {})
        response = await self.client.get("/api/v1/capital/collateral", headers=headers)
        response.raise_for_status()
        return response.json()

    async def get_markets(self) -> List[Dict[str, Any]]:
        response = await self.client.get("/api/v1/markets")
        response.raise_for_status()
        return response.json()

    async def get_market_depth(self, symbol: str) -> Dict[str, Any]:
        response = await self.client.get(f"/api/v1/depth?symbol={symbol}")
        response.raise_for_status()
        return response.json()

    async def get_positions(self) -> List[Dict[str, Any]]:
        headers = self._get_headers("positionQuery", {})
        response = await self.client.get("/api/v1/positions", headers=headers)
        response.raise_for_status()
        return response.json()

    async def place_order(
        self,
        symbol: str,
        side: str,  # "Bid" or "Ask"
        order_type: str,  # "Limit", "Market"
        quantity: str,
        price: Optional[str] = None,
        time_in_force: str = "GTC",
        reduce_only: bool = False,
        client_id: Optional[int] = None
    ) -> Dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "quantity": quantity,
            "timeInForce": time_in_force,
        }
        
        if price:
            params["price"] = price
        if reduce_only:
            params["reduceOnly"] = reduce_only
        if client_id:
            params["clientId"] = client_id

        headers = self._get_headers("orderExecute", params)
        
        response = await self.client.post(
            "/api/v1/order", 
            json=params,
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    async def place_batch_orders(self, orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        # For batch orders, each order needs to be signed separately
        signing_parts = []
        for order in orders:
            sorted_params = sorted(order.items())
            param_string = urlencode(sorted_params)
            signing_parts.append(f"instruction=orderExecute&{param_string}")
        
        timestamp = int(time.time() * 1000)
        signing_string = "&".join(signing_parts) + f"&timestamp={timestamp}&window={self.config.window_ms}"
        
        signature = self.private_key.sign(
            signing_string.encode(), encoder=nacl.encoding.Base64Encoder
        )
        
        headers = {
            "X-API-Key": base64.b64encode(self.verify_key.encode()).decode(),
            "X-Signature": signature.signature.decode(),
            "X-Timestamp": str(timestamp),
            "X-Window": str(self.config.window_ms),
            "Content-Type": "application/json"
        }
        
        response = await self.client.post(
            "/api/v1/orders",
            json=orders,
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        params = {"orderId": order_id, "symbol": symbol}
        headers = self._get_headers("orderCancel", params)
        
        response = await self.client.delete(
            "/api/v1/order",
            json=params,
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
            
        headers = self._get_headers("orderHistoryQueryAll", params)
        response = await self.client.get("/api/v1/history/orders", headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    async def get_fill_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
            
        headers = self._get_headers("fillHistoryQueryAll", params)
        response = await self.client.get("/api/v1/history/fills", headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()