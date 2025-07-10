import asyncio
import logging
import os
import sys
from typing import Optional
import signal
from dotenv import load_dotenv

from src.api import BackpackClient, BackpackConfig, BackpackWebSocket
from src.data import MarketDataCollector
from src.strategy import SpotFuturesArbitrage
from src.risk import RiskManager, RiskLimits


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arbitrage.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class ArbitrageBot:
    def __init__(self):
        self.client: Optional[BackpackClient] = None
        self.websocket: Optional[BackpackWebSocket] = None
        self.market_data: Optional[MarketDataCollector] = None
        self.risk_manager: Optional[RiskManager] = None
        self.strategy: Optional[SpotFuturesArbitrage] = None
        self.running = False

    async def initialize(self):
        # Load configuration from environment variables
        api_key = os.getenv("BACKPACK_API_KEY")
        private_key = os.getenv("BACKPACK_PRIVATE_KEY")
        
        if not api_key or not private_key:
            logger.error("Missing required environment variables: BACKPACK_API_KEY, BACKPACK_PRIVATE_KEY")
            raise ValueError("Missing API credentials")
        
        # Initialize configuration
        config = BackpackConfig(
            api_key=api_key,
            private_key=private_key
        )
        
        # Initialize components
        self.client = BackpackClient(config)
        self.websocket = BackpackWebSocket(config)
        self.market_data = MarketDataCollector(self.client, self.websocket)
        
        # Initialize risk manager with custom limits
        risk_limits = RiskLimits(
            max_position_size_usd=float(os.getenv("MAX_POSITION_SIZE", "1000")),
            max_total_exposure_usd=float(os.getenv("MAX_TOTAL_EXPOSURE", "10000")),
            max_daily_loss_usd=float(os.getenv("MAX_DAILY_LOSS", "500")),
            min_account_balance_usd=float(os.getenv("MIN_ACCOUNT_BALANCE", "2000"))
        )
        self.risk_manager = RiskManager(risk_limits)
        
        # Initialize strategy
        self.strategy = SpotFuturesArbitrage(
            self.client, 
            self.market_data, 
            self.risk_manager
        )
        
        # Configure strategy parameters from environment
        self.strategy.min_spread_threshold = float(os.getenv("MIN_SPREAD_THRESHOLD", "0.1"))
        self.strategy.max_spread_threshold = float(os.getenv("MAX_SPREAD_THRESHOLD", "2.0"))
        self.strategy.min_funding_rate = float(os.getenv("MIN_FUNDING_RATE", "0.01"))
        self.strategy.position_size_usd = float(os.getenv("POSITION_SIZE_USD", "1000"))
        self.strategy.min_execution_threshold = float(os.getenv("MIN_EXECUTION_THRESHOLD", "0.15"))
        self.strategy.confidence_threshold = float(os.getenv("CONFIDENCE_THRESHOLD", "0.7"))
        self.strategy.large_spread_threshold = float(os.getenv("LARGE_SPREAD_THRESHOLD", "0.40"))
        self.strategy.use_maker_for_large_spreads = os.getenv("USE_MAKER_FOR_LARGE_SPREADS", "true").lower() == "true"
        self.strategy.order_timeout = int(os.getenv("ORDER_TIMEOUT_SECONDS", "15"))
        self.strategy.fallback_to_taker = os.getenv("FALLBACK_TO_TAKER", "true").lower() == "true"
        
        logger.info("Arbitrage bot initialized successfully")

    async def start(self):
        try:
            # Start WebSocket connection
            await self.websocket.start()
            
            # Initialize market data collector
            await self.market_data.initialize()
            
            # Initialize strategy
            await self.strategy.initialize()
            
            # Define symbols to monitor (can be expanded)
            symbols_to_monitor = [
                "SOL_USDC", "SOL_USDC_PERP",
                "BTC_USDC", "BTC_USDC_PERP", 
                "ETH_USDC", "ETH_USDC_PERP",
                "SUI_USDC", "SUI_USDC_PERP",
                "JUP_USDC", "JUP_USDC_PERP"
            ]
            
            # Start market data collection
            await self.market_data.start_data_collection(symbols_to_monitor)
            
            # Wait a bit for initial market data
            await asyncio.sleep(10)
            
            # Check account health before starting
            balances = await self.client.get_balances()
            collateral = await self.client.get_collateral()
            health = self.risk_manager.check_position_health(balances, collateral)
            logger.info(f"Account health: {health['risk_status']} (score: {health['health_score']:.2f})")
            logger.info(f"Net equity: ${float(collateral.get('netEquity', 0)):.2f}")
            
            if health['risk_status'] == 'CRITICAL':
                logger.error("Account health is critical, cannot start trading")
                return
            
            self.running = True
            logger.info("Starting arbitrage strategy...")
            
            # Start strategy in background
            strategy_task = asyncio.create_task(self.strategy.start_strategy())
            
            # Start monitoring loop
            monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            # Wait for tasks
            await asyncio.gather(strategy_task, monitoring_task, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error starting arbitrage bot: {e}")
            raise

    async def _monitoring_loop(self):
        while self.running:
            try:
                # Log strategy statistics
                stats = self.strategy.get_strategy_stats()
                logger.info(f"Strategy stats: {stats}")
                
                # Log risk metrics
                risk_metrics = self.risk_manager.get_risk_metrics()
                logger.info(f"Risk metrics: {risk_metrics}")
                
                # Log current exposure
                exposure = self.risk_manager.get_current_exposure()
                if exposure['total_exposure_usd'] > 0:
                    logger.info(f"Current exposure: ${exposure['total_exposure_usd']:.2f}, "
                              f"P&L: ${exposure['total_pnl']:.2f}")
                
                # Check for opportunities
                opportunities = await self.strategy.scan_opportunities()
                if opportunities:
                    logger.info(f"Found {len(opportunities)} arbitrage opportunities")
                    for opp in opportunities[:3]:  # Log top 3
                        logger.info(f"  {opp.spot_symbol}/{opp.futures_symbol}: "
                                  f"{opp.spread_percentage:.3f}% spread, "
                                  f"{opp.expected_return:.2f}% expected return")
                
                await asyncio.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)

    async def stop(self):
        logger.info("Stopping arbitrage bot...")
        self.running = False
        
        if self.strategy:
            await self.strategy.stop_strategy()
        
        if self.market_data:
            await self.market_data.stop()
        
        if self.websocket:
            await self.websocket.disconnect()
        
        if self.client:
            await self.client.close()
        
        logger.info("Arbitrage bot stopped")


async def main():
    # Load environment variables from .env file
    load_dotenv()
    
    bot = ArbitrageBot()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(bot.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await bot.initialize()
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
