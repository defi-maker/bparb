import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd

from ..api import BackpackClient, BackpackWebSocket


logger = logging.getLogger(__name__)


@dataclass
class MarketData:
    symbol: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    last_price: float
    timestamp: datetime
    is_futures: bool = False
    funding_rate: Optional[float] = None
    mark_price: Optional[float] = None


@dataclass
class OrderBook:
    symbol: str
    bids: List[tuple[float, float]]  # (price, size)
    asks: List[tuple[float, float]]  # (price, size)
    timestamp: datetime


class MarketDataCollector:
    def __init__(self, client: BackpackClient, websocket: BackpackWebSocket):
        self.client = client
        self.websocket = websocket
        self.market_data: Dict[str, MarketData] = {}
        self.order_books: Dict[str, OrderBook] = {}
        self.markets_info: Dict[str, Dict[str, Any]] = {}
        self.running = False

    async def initialize(self):
        # Get all available markets
        markets = await self.client.get_markets()
        
        for market in markets:
            symbol = market["symbol"]
            self.markets_info[symbol] = market
            
            # Initialize market data
            self.market_data[symbol] = MarketData(
                symbol=symbol,
                bid_price=0.0,
                ask_price=0.0,
                bid_size=0.0,
                ask_size=0.0,
                last_price=0.0,
                timestamp=datetime.now(),
                is_futures=market.get("marketType") == "PERP",
                funding_rate=market.get("fundingRate"),
                mark_price=market.get("markPrice")
            )
        
        logger.info(f"Initialized {len(markets)} markets")

    async def start_data_collection(self, symbols: List[str]):
        # First, get initial depth snapshots for all symbols
        for symbol in symbols:
            try:
                depth_data = await self.client.get_market_depth(symbol)
                
                # Process initial depth snapshot
                bids = [(float(bid[0]), float(bid[1])) for bid in depth_data.get("bids", [])]
                asks = [(float(ask[0]), float(ask[1])) for ask in depth_data.get("asks", [])]
                
                # Create order book
                self.order_books[symbol] = OrderBook(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=datetime.now()
                )
                
                # Update market data with best bid/ask
                if symbol in self.market_data and bids and asks:
                    self.market_data[symbol].bid_price = bids[0][0]
                    self.market_data[symbol].ask_price = asks[0][0]
                    self.market_data[symbol].bid_size = bids[0][1]
                    self.market_data[symbol].ask_size = asks[0][1]
                    
                    logger.info(f"Initialized {symbol} prices: bid={bids[0][0]}, ask={asks[0][0]}")
                
            except Exception as e:
                logger.error(f"Failed to get initial depth for {symbol}: {e}")
        
        # Subscribe to ticker streams for price updates
        ticker_streams = [f"ticker.{symbol}" for symbol in symbols]
        
        # Subscribe to depth streams for order book data
        depth_streams = [f"depth.{symbol}" for symbol in symbols]
        
        # Add handlers
        for symbol in symbols:
            self.websocket.add_handler(
                f"ticker.{symbol}", 
                lambda data, s=symbol: asyncio.create_task(self._handle_ticker(s, data))
            )
            self.websocket.add_handler(
                f"depth.{symbol}",
                lambda data, s=symbol: asyncio.create_task(self._handle_depth(s, data))
            )
        
        # Subscribe to streams
        await self.websocket.subscribe_public(ticker_streams + depth_streams)
        
        self.running = True
        logger.info(f"Started data collection for {len(symbols)} symbols")

    async def _handle_ticker(self, symbol: str, data: Dict[str, Any]):
        try:
            if symbol not in self.market_data:
                return
                
            market_data = self.market_data[symbol]
            
            # Update market data from ticker (ticker doesn't have bid/ask, only close price)
            if "c" in data:
                market_data.last_price = float(data["c"])
            
            market_data.timestamp = datetime.now()
            
            # Update funding rate for futures
            if market_data.is_futures and "f" in data:
                market_data.funding_rate = float(data["f"])
            
        except Exception as e:
            logger.error(f"Error handling ticker for {symbol}: {e}")

    async def _handle_depth(self, symbol: str, data: Dict[str, Any]):
        try:
            # Get current order book or create new one
            if symbol not in self.order_books:
                self.order_books[symbol] = OrderBook(
                    symbol=symbol,
                    bids=[],
                    asks=[],
                    timestamp=datetime.now()
                )
            
            order_book = self.order_books[symbol]
            
            # Process bid updates
            for bid_update in data.get("b", []):
                price = float(bid_update[0])
                size = float(bid_update[1])
                
                if size == 0.0:
                    # Remove this price level
                    order_book.bids = [(p, s) for p, s in order_book.bids if p != price]
                else:
                    # Update or add this price level
                    # Remove existing price level if it exists
                    order_book.bids = [(p, s) for p, s in order_book.bids if p != price]
                    # Add new price level and sort by price descending (highest bid first)
                    order_book.bids.append((price, size))
                    order_book.bids.sort(key=lambda x: x[0], reverse=True)
            
            # Process ask updates
            for ask_update in data.get("a", []):
                price = float(ask_update[0])
                size = float(ask_update[1])
                
                if size == 0.0:
                    # Remove this price level
                    order_book.asks = [(p, s) for p, s in order_book.asks if p != price]
                else:
                    # Update or add this price level
                    # Remove existing price level if it exists
                    order_book.asks = [(p, s) for p, s in order_book.asks if p != price]
                    # Add new price level and sort by price ascending (lowest ask first)
                    order_book.asks.append((price, size))
                    order_book.asks.sort(key=lambda x: x[0])
            
            # Update market data with best bid/ask
            if symbol in self.market_data:
                if order_book.bids:
                    self.market_data[symbol].bid_price = order_book.bids[0][0]
                    self.market_data[symbol].bid_size = order_book.bids[0][1]
                
                if order_book.asks:
                    self.market_data[symbol].ask_price = order_book.asks[0][0]
                    self.market_data[symbol].ask_size = order_book.asks[0][1]
            
            order_book.timestamp = datetime.now()
            
        except Exception as e:
            logger.error(f"Error handling depth for {symbol}: {e}")

    def get_market_data(self, symbol: str) -> Optional[MarketData]:
        return self.market_data.get(symbol)

    def get_order_book(self, symbol: str) -> Optional[OrderBook]:
        return self.order_books.get(symbol)

    def get_spread(self, symbol: str) -> Optional[float]:
        if symbol not in self.market_data:
            return None
        
        market = self.market_data[symbol]
        if market.bid_price > 0 and market.ask_price > 0:
            return market.ask_price - market.bid_price
        return None

    def get_spread_percentage(self, symbol: str) -> Optional[float]:
        if symbol not in self.market_data:
            return None
        
        market = self.market_data[symbol]
        if market.bid_price > 0 and market.ask_price > 0:
            mid_price = (market.bid_price + market.ask_price) / 2
            spread = market.ask_price - market.bid_price
            return (spread / mid_price) * 100
        return None

    def is_market_futures(self, symbol: str) -> bool:
        return self.market_data.get(symbol, MarketData("", 0, 0, 0, 0, 0, datetime.now())).is_futures

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        if not self.is_market_futures(symbol):
            return None
        return self.market_data.get(symbol, MarketData("", 0, 0, 0, 0, 0, datetime.now())).funding_rate

    async def get_historical_data(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        # Note: Backpack API doesn't have historical price data endpoint
        # This would need to be implemented using kline/candlestick data if available
        # For now, return empty DataFrame
        return pd.DataFrame()

    async def stop(self):
        self.running = False
        logger.info("Stopped market data collection")