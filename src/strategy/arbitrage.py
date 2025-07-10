import asyncio
import logging
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from ..api import BackpackClient
from ..data import MarketDataCollector, MarketData
from ..risk import RiskManager


logger = logging.getLogger(__name__)


class ArbitrageType(Enum):
    SPOT_FUTURES = "spot_futures"
    FUNDING_RATE = "funding_rate"


@dataclass
class ArbitrageOpportunity:
    type: ArbitrageType
    spot_symbol: str
    futures_symbol: str
    spot_price: float
    futures_price: float
    spread: float
    spread_percentage: float
    funding_rate: Optional[float]
    expected_return: float
    confidence_score: float
    timestamp: datetime
    

@dataclass
class ArbitragePosition:
    opportunity: ArbitrageOpportunity
    spot_order_id: Optional[str] = None
    futures_order_id: Optional[str] = None
    spot_quantity: float = 0.0
    futures_quantity: float = 0.0
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    realized_pnl: float = 0.0
    status: str = "pending"  # pending, active, closed


class SpotFuturesArbitrage:
    def __init__(
        self, 
        client: BackpackClient, 
        market_data: MarketDataCollector,
        risk_manager: Optional['RiskManager'] = None
    ):
        self.client = client
        self.market_data = market_data
        self.risk_manager = risk_manager
        
        # Strategy parameters
        self.min_spread_threshold = 0.1  # Minimum spread percentage to trigger
        self.max_spread_threshold = 2.0  # Maximum spread to avoid illiquid markets
        self.min_funding_rate = 0.01  # Minimum funding rate for funding arbitrage
        self.position_size_usd = 1000  # Default position size in USD
        self.min_execution_threshold = 0.15  # Minimum expected return to execute trade
        self.confidence_threshold = 0.7  # Minimum confidence score to execute trade
        self.large_spread_threshold = 0.40  # Use maker orders above this spread
        self.use_maker_for_large_spreads = True  # Dynamic order type selection
        self.order_timeout = 15  # Timeout for limit orders in seconds
        self.fallback_to_taker = True  # Use market orders if limit orders fail
        
        # Active positions and opportunities
        self.active_positions: Dict[str, ArbitragePosition] = {}
        self.opportunities: List[ArbitrageOpportunity] = []
        
        # Market pairs to monitor
        self.market_pairs: List[Tuple[str, str]] = []
        
        self.running = False

    async def initialize(self):
        # Discover spot-futures pairs
        await self._discover_market_pairs()
        logger.info(f"Found {len(self.market_pairs)} spot-futures pairs to monitor")

    async def _discover_market_pairs(self):
        # Get all markets and find spot-futures pairs
        for symbol, market_data in self.market_data.market_data.items():
            if market_data.is_futures and symbol.endswith("_PERP"):
                # Find corresponding spot market
                base_symbol = symbol.replace("_PERP", "")
                if base_symbol in self.market_data.market_data:
                    self.market_pairs.append((base_symbol, symbol))

    async def scan_opportunities(self) -> List[ArbitrageOpportunity]:
        opportunities = []
        
        for spot_symbol, futures_symbol in self.market_pairs:
            opportunity = await self._analyze_pair(spot_symbol, futures_symbol)
            if opportunity:
                opportunities.append(opportunity)
                logger.info(f"Found opportunity: {spot_symbol}/{futures_symbol} - {opportunity.spread_percentage:.4f}% spread")
        
        self.opportunities = opportunities
        return opportunities

    async def _analyze_pair(self, spot_symbol: str, futures_symbol: str) -> Optional[ArbitrageOpportunity]:
        spot_data = self.market_data.get_market_data(spot_symbol)
        futures_data = self.market_data.get_market_data(futures_symbol)
        
        if not spot_data or not futures_data:
            return None
        
        # Calculate mid prices
        spot_mid = (spot_data.bid_price + spot_data.ask_price) / 2
        futures_mid = (futures_data.bid_price + futures_data.ask_price) / 2
        
        if spot_mid <= 0 or futures_mid <= 0:
            return None
        
        # Calculate spread
        spread = futures_mid - spot_mid
        spread_percentage = (spread / spot_mid) * 100
        
        # Skip if spread is too small or too large
        if abs(spread_percentage) < self.min_spread_threshold:
            return None
        if abs(spread_percentage) > self.max_spread_threshold:
            return None
        
        # Determine arbitrage type and expected return
        arb_type = ArbitrageType.SPOT_FUTURES
        expected_return = abs(spread_percentage)
        
        # Check for funding rate arbitrage
        funding_rate = futures_data.funding_rate
        if funding_rate and abs(funding_rate) > self.min_funding_rate:
            arb_type = ArbitrageType.FUNDING_RATE
            # Annualize funding rate (8 hour periods)
            annualized_funding = funding_rate * 365 * 3  # 3 times per day
            expected_return = abs(annualized_funding) * 100
        
        # Calculate confidence score based on liquidity and spread stability
        confidence_score = self._calculate_confidence(spot_data, futures_data, spread_percentage)
        
        return ArbitrageOpportunity(
            type=arb_type,
            spot_symbol=spot_symbol,
            futures_symbol=futures_symbol,
            spot_price=spot_mid,
            futures_price=futures_mid,
            spread=spread,
            spread_percentage=spread_percentage,
            funding_rate=funding_rate,
            expected_return=expected_return,
            confidence_score=confidence_score,
            timestamp=datetime.now()
        )

    def _calculate_confidence(self, spot_data: MarketData, futures_data: MarketData, spread_pct: float) -> float:
        # Basic confidence calculation based on:
        # 1. Order book depth (bid/ask sizes)
        # 2. Spread magnitude
        # 3. Data freshness
        
        confidence = 0.5  # Base confidence
        
        # Liquidity factor (higher is better)
        min_size = min(spot_data.bid_size, spot_data.ask_size, 
                      futures_data.bid_size, futures_data.ask_size)
        
        # Calculate required size based on position size
        required_size = self.position_size_usd / max(spot_data.bid_price, spot_data.ask_price)
        
        if min_size > required_size * 10:  # 10x buffer - excellent liquidity
            confidence += 0.3
        elif min_size > required_size * 3:  # 3x buffer - good liquidity  
            confidence += 0.2
        elif min_size > required_size:     # 1x buffer - adequate liquidity
            confidence += 0.1
        
        # Spread factor (moderate spreads are better)
        if 0.2 <= abs(spread_pct) <= 1.0:
            confidence += 0.2
        elif 0.15 <= abs(spread_pct) < 0.2:  # Smaller but reasonable spreads
            confidence += 0.1
        
        return min(confidence, 1.0)

    async def execute_arbitrage(self, opportunity: ArbitrageOpportunity) -> Optional[ArbitragePosition]:
        if self.risk_manager and not await self.risk_manager.check_risk_limits(opportunity):
            logger.warning(f"Risk limits exceeded for {opportunity.spot_symbol}/{opportunity.futures_symbol}")
            return None
        
        # Calculate position sizes
        spot_quantity, futures_quantity = self._calculate_position_sizes(opportunity)
        
        if spot_quantity <= 0 or futures_quantity <= 0:
            logger.warning(f"Invalid position sizes calculated for {opportunity.spot_symbol}")
            return None
        
        try:
            # Determine trade direction and prices
            if opportunity.spread > 0:  # Futures premium
                # Buy spot at ask, sell futures at bid (to get maker fees)
                spot_side = "Bid"
                futures_side = "Ask"
                spot_price = opportunity.spot_price * 1.001  # Slightly above current bid for maker
                futures_price = opportunity.futures_price * 0.999  # Slightly below current ask for maker
            else:  # Spot premium
                # Sell spot at bid, buy futures at ask (to get maker fees)
                spot_side = "Ask"
                futures_side = "Bid"
                spot_price = opportunity.spot_price * 0.999  # Slightly below current ask for maker
                futures_price = opportunity.futures_price * 1.001  # Slightly above current bid for maker
            
            # Decide order type based on spread size
            use_maker = (self.use_maker_for_large_spreads and 
                        abs(opportunity.spread_percentage) >= self.large_spread_threshold)
            
            if use_maker:
                # Execute trades using limit orders for maker fees (large spreads only)
                orders = [
                    {
                        "symbol": opportunity.spot_symbol,
                        "side": spot_side,
                        "orderType": "Limit",
                        "quantity": str(spot_quantity),
                        "price": str(round(spot_price, 6)),
                        "timeInForce": "GTC"  # Good Till Cancel
                    },
                    {
                        "symbol": opportunity.futures_symbol,
                        "side": futures_side,
                        "orderType": "Limit",
                        "quantity": str(futures_quantity),
                        "price": str(round(futures_price, 6)),
                        "timeInForce": "GTC"
                    }
                ]
                
                logger.info(f"Placing MAKER orders (spread {opportunity.spread_percentage:.3f}%): "
                          f"spot={spot_side}@{spot_price:.6f}, futures={futures_side}@{futures_price:.6f}")
                result = await self.client.place_batch_orders(orders)
                
                # Wait for orders to fill or timeout
                await asyncio.sleep(self.order_timeout)
                
                # TODO: Check if orders filled, if not and fallback enabled, use market orders
                
            else:
                # Execute trades immediately using market orders (small spreads)
                orders = [
                    {
                        "symbol": opportunity.spot_symbol,
                        "side": spot_side,
                        "orderType": "Market",
                        "quantity": str(spot_quantity),
                        "timeInForce": "IOC"  # Immediate or Cancel
                    },
                    {
                        "symbol": opportunity.futures_symbol,
                        "side": futures_side,
                        "orderType": "Market",
                        "quantity": str(futures_quantity),
                        "timeInForce": "IOC"
                    }
                ]
                
                logger.info(f"Placing TAKER orders (spread {opportunity.spread_percentage:.3f}%): "
                          f"spot={spot_side}, futures={futures_side}")
                result = await self.client.place_batch_orders(orders)
            
            # Create position record
            position = ArbitragePosition(
                opportunity=opportunity,
                spot_quantity=spot_quantity,
                futures_quantity=futures_quantity,
                entry_time=datetime.now(),
                status="active"
            )
            
            # Extract order IDs from result if available
            if isinstance(result, list) and len(result) >= 2:
                position.spot_order_id = result[0].get("orderId")
                position.futures_order_id = result[1].get("orderId")
            
            # Store position
            position_key = f"{opportunity.spot_symbol}_{opportunity.futures_symbol}_{int(datetime.now().timestamp())}"
            self.active_positions[position_key] = position
            
            logger.info(f"Executed arbitrage: {opportunity.spot_symbol}/{opportunity.futures_symbol}, "
                       f"spread: {opportunity.spread_percentage:.3f}%")
            
            return position
            
        except Exception as e:
            logger.error(f"Failed to execute arbitrage for {opportunity.spot_symbol}: {e}")
            return None

    def _calculate_position_sizes(self, opportunity: ArbitrageOpportunity) -> Tuple[float, float]:
        # Calculate position sizes based on USD allocation
        spot_price = opportunity.spot_price
        futures_price = opportunity.futures_price
        
        if spot_price <= 0 or futures_price <= 0:
            return 0.0, 0.0
        
        # Calculate base quantity from USD position size
        base_quantity = self.position_size_usd / spot_price
        
        # For most cases, spot and futures quantities should be equal
        spot_quantity = base_quantity
        futures_quantity = base_quantity
        
        # Apply risk manager position sizing if available
        if self.risk_manager:
            max_size = self.risk_manager.get_max_position_size(opportunity.spot_symbol)
            spot_quantity = min(spot_quantity, max_size)
            futures_quantity = min(futures_quantity, max_size)
        
        return spot_quantity, futures_quantity

    async def monitor_positions(self):
        for position_key, position in list(self.active_positions.items()):
            if position.status != "active":
                continue
            
            # Check if position should be closed
            should_close = await self._should_close_position(position)
            
            if should_close:
                await self._close_position(position_key, position)

    async def _should_close_position(self, position: ArbitragePosition) -> bool:
        # Check time-based exit (e.g., hold for funding payment)
        if position.opportunity.type == ArbitrageType.FUNDING_RATE:
            # Close after next funding payment (8 hours)
            if position.entry_time and datetime.now() - position.entry_time > timedelta(hours=8.5):
                return True
        
        # Check if spread has converged or reversed significantly
        current_opportunity = await self._analyze_pair(
            position.opportunity.spot_symbol,
            position.opportunity.futures_symbol
        )
        
        if current_opportunity:
            # Close if spread has reversed or reduced significantly
            original_spread = position.opportunity.spread_percentage
            current_spread = current_opportunity.spread_percentage
            
            if abs(current_spread) < 0.05:  # Spread converged
                return True
            
            if original_spread * current_spread < 0:  # Spread reversed
                return True
        
        return False

    async def _close_position(self, position_key: str, position: ArbitragePosition):
        try:
            # Create closing orders (reverse the original trades)
            if position.opportunity.spread > 0:  # Original: buy spot, sell futures
                # Close: sell spot, buy futures
                spot_side = "Ask"
                futures_side = "Bid"
            else:  # Original: sell spot, buy futures
                # Close: buy spot, sell futures
                spot_side = "Bid"
                futures_side = "Ask"
            
            orders = [
                {
                    "symbol": position.opportunity.spot_symbol,
                    "side": spot_side,
                    "orderType": "Market",
                    "quantity": str(position.spot_quantity),
                    "timeInForce": "IOC",
                    "reduceOnly": True
                },
                {
                    "symbol": position.opportunity.futures_symbol,
                    "side": futures_side,
                    "orderType": "Market",
                    "quantity": str(position.futures_quantity),
                    "timeInForce": "IOC",
                    "reduceOnly": True
                }
            ]
            
            await self.client.place_batch_orders(orders)
            
            position.exit_time = datetime.now()
            position.status = "closed"
            
            logger.info(f"Closed arbitrage position: {position.opportunity.spot_symbol}/{position.opportunity.futures_symbol}")
            
        except Exception as e:
            logger.error(f"Failed to close position {position_key}: {e}")

    async def start_strategy(self):
        self.running = True
        logger.info("Starting spot-futures arbitrage strategy")
        
        while self.running:
            try:
                # Scan for opportunities
                opportunities = await self.scan_opportunities()
                
                # Execute profitable opportunities
                for opportunity in opportunities:
                    logger.info(f"Checking execution for {opportunity.spot_symbol}/{opportunity.futures_symbol}: "
                              f"confidence={opportunity.confidence_score:.3f}, "
                              f"expected_return={opportunity.expected_return:.3f}%, "
                              f"thresholds: confidence>{self.confidence_threshold}, return>{self.min_execution_threshold}")
                    if opportunity.confidence_score > self.confidence_threshold and opportunity.expected_return > self.min_execution_threshold:
                        await self.execute_arbitrage(opportunity)
                
                # Monitor existing positions
                await self.monitor_positions()
                
                # Wait before next scan
                await asyncio.sleep(5)  # Scan every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in arbitrage strategy: {e}")
                await asyncio.sleep(10)

    async def stop_strategy(self):
        self.running = False
        
        # Close all active positions
        for position_key, position in list(self.active_positions.items()):
            if position.status == "active":
                await self._close_position(position_key, position)
        
        logger.info("Stopped spot-futures arbitrage strategy")

    def get_strategy_stats(self) -> Dict[str, Any]:
        total_positions = len(self.active_positions)
        active_positions = sum(1 for p in self.active_positions.values() if p.status == "active")
        closed_positions = sum(1 for p in self.active_positions.values() if p.status == "closed")
        
        total_pnl = sum(p.realized_pnl for p in self.active_positions.values() if p.status == "closed")
        
        return {
            "total_positions": total_positions,
            "active_positions": active_positions,
            "closed_positions": closed_positions,
            "total_realized_pnl": total_pnl,
            "current_opportunities": len(self.opportunities),
            "monitored_pairs": len(self.market_pairs)
        }