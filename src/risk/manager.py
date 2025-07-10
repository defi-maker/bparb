import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


@dataclass
class RiskLimits:
    max_position_size_usd: float = 10000  # Maximum position size per trade
    max_total_exposure_usd: float = 50000  # Maximum total exposure across all positions
    max_positions_per_symbol: int = 3  # Maximum number of positions per symbol
    max_daily_loss_usd: float = 1000  # Maximum daily loss limit
    max_leverage: float = 3.0  # Maximum leverage allowed
    min_account_balance_usd: float = 5000  # Minimum account balance to maintain


@dataclass
class PositionRisk:
    symbol: str
    size_usd: float
    pnl: float
    leverage: float
    timestamp: datetime


class RiskManager:
    def __init__(self, limits: Optional[RiskLimits] = None):
        self.limits = limits or RiskLimits()
        self.positions: Dict[str, PositionRisk] = {}
        self.daily_pnl = 0.0
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
    async def check_risk_limits(self, opportunity) -> bool:
        # Check if we can take this arbitrage opportunity
        
        # 1. Check maximum position size
        position_size_usd = getattr(opportunity, 'position_size_usd', 1000)
        if position_size_usd > self.limits.max_position_size_usd:
            logger.warning(f"Position size {position_size_usd} exceeds limit {self.limits.max_position_size_usd}")
            return False
        
        # 2. Check total exposure
        current_exposure = sum(pos.size_usd for pos in self.positions.values())
        if current_exposure + position_size_usd > self.limits.max_total_exposure_usd:
            logger.warning(f"Total exposure would exceed limit: {current_exposure + position_size_usd} > {self.limits.max_total_exposure_usd}")
            return False
        
        # 3. Check positions per symbol
        symbol = opportunity.spot_symbol
        symbol_positions = sum(1 for pos in self.positions.values() if pos.symbol == symbol)
        if symbol_positions >= self.limits.max_positions_per_symbol:
            logger.warning(f"Too many positions for {symbol}: {symbol_positions} >= {self.limits.max_positions_per_symbol}")
            return False
        
        # 4. Check daily loss limit
        await self._update_daily_pnl()
        if self.daily_pnl < -self.limits.max_daily_loss_usd:
            logger.warning(f"Daily loss limit exceeded: {self.daily_pnl} < -{self.limits.max_daily_loss_usd}")
            return False
        
        return True
    
    async def _update_daily_pnl(self):
        # Reset daily P&L if it's a new day
        now = datetime.now()
        if now.date() > self.daily_reset_time.date():
            self.daily_pnl = 0.0
            self.daily_reset_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def add_position(self, symbol: str, size_usd: float, leverage: float = 1.0):
        position_id = f"{symbol}_{int(datetime.now().timestamp())}"
        self.positions[position_id] = PositionRisk(
            symbol=symbol,
            size_usd=size_usd,
            pnl=0.0,
            leverage=leverage,
            timestamp=datetime.now()
        )
        logger.info(f"Added position: {position_id}, size: ${size_usd}")
    
    def update_position_pnl(self, position_id: str, pnl: float):
        if position_id in self.positions:
            old_pnl = self.positions[position_id].pnl
            self.positions[position_id].pnl = pnl
            self.daily_pnl += (pnl - old_pnl)
    
    def remove_position(self, position_id: str) -> Optional[float]:
        if position_id in self.positions:
            position = self.positions.pop(position_id)
            logger.info(f"Removed position: {position_id}, final PnL: ${position.pnl}")
            return position.pnl
        return None
    
    def get_max_position_size(self, symbol: str) -> float:
        # Calculate maximum position size considering current exposure
        current_exposure = sum(pos.size_usd for pos in self.positions.values())
        remaining_capacity = self.limits.max_total_exposure_usd - current_exposure
        
        # Also consider per-position limit
        max_size = min(self.limits.max_position_size_usd, remaining_capacity)
        
        return max(0, max_size)
    
    def get_current_exposure(self) -> Dict[str, Any]:
        total_exposure = sum(pos.size_usd for pos in self.positions.values())
        total_pnl = sum(pos.pnl for pos in self.positions.values())
        
        symbol_exposure = {}
        for pos in self.positions.values():
            if pos.symbol not in symbol_exposure:
                symbol_exposure[pos.symbol] = {"size_usd": 0, "pnl": 0, "count": 0}
            symbol_exposure[pos.symbol]["size_usd"] += pos.size_usd
            symbol_exposure[pos.symbol]["pnl"] += pos.pnl
            symbol_exposure[pos.symbol]["count"] += 1
        
        return {
            "total_exposure_usd": total_exposure,
            "total_pnl": total_pnl,
            "daily_pnl": self.daily_pnl,
            "position_count": len(self.positions),
            "symbol_breakdown": symbol_exposure,
            "risk_utilization": {
                "exposure_pct": (total_exposure / self.limits.max_total_exposure_usd) * 100,
                "daily_loss_pct": (abs(min(0, self.daily_pnl)) / self.limits.max_daily_loss_usd) * 100
            }
        }
    
    def check_position_health(self, client_balances: Dict[str, Any], collateral_info: Dict[str, Any] = None) -> Dict[str, Any]:
        # Check overall account health using collateral info if available
        if collateral_info and 'netEquity' in collateral_info:
            # Use net equity from collateral info (includes lending/borrowing)
            total_balance_usd = float(collateral_info['netEquity'])
        else:
            # Fallback to simple balance calculation
            total_balance_usd = sum(float(balances.get("available", 0)) for balances in client_balances.values())
        
        health_score = 1.0
        warnings = []
        
        # Check minimum balance
        if total_balance_usd < self.limits.min_account_balance_usd:
            health_score *= 0.5
            warnings.append(f"Account balance ${total_balance_usd:.2f} below minimum ${self.limits.min_account_balance_usd}")
        
        # Check daily loss
        if self.daily_pnl < -self.limits.max_daily_loss_usd * 0.8:
            health_score *= 0.7
            warnings.append(f"Daily loss ${self.daily_pnl} approaching limit ${-self.limits.max_daily_loss_usd}")
        
        # Check exposure
        current_exposure = sum(pos.size_usd for pos in self.positions.values())
        if current_exposure > self.limits.max_total_exposure_usd * 0.8:
            health_score *= 0.8
            warnings.append(f"Exposure ${current_exposure} approaching limit ${self.limits.max_total_exposure_usd}")
        
        return {
            "health_score": health_score,
            "account_balance_usd": total_balance_usd,
            "warnings": warnings,
            "risk_status": "HEALTHY" if health_score > 0.8 else "WARNING" if health_score > 0.5 else "CRITICAL"
        }
    
    def get_risk_metrics(self) -> Dict[str, Any]:
        if not self.positions:
            return {
                "total_positions": 0,
                "total_exposure": 0,
                "average_leverage": 0,
                "max_single_position": 0,
                "risk_concentration": {}
            }
        
        total_exposure = sum(pos.size_usd for pos in self.positions.values())
        leverages = [pos.leverage for pos in self.positions.values()]
        sizes = [pos.size_usd for pos in self.positions.values()]
        
        # Calculate symbol concentration
        symbol_concentration = {}
        for pos in self.positions.values():
            if pos.symbol not in symbol_concentration:
                symbol_concentration[pos.symbol] = 0
            symbol_concentration[pos.symbol] += pos.size_usd
        
        # Convert to percentages
        for symbol in symbol_concentration:
            symbol_concentration[symbol] = (symbol_concentration[symbol] / total_exposure) * 100 if total_exposure > 0 else 0
        
        return {
            "total_positions": len(self.positions),
            "total_exposure": total_exposure,
            "average_leverage": sum(leverages) / len(leverages) if leverages else 0,
            "max_single_position": max(sizes) if sizes else 0,
            "risk_concentration": symbol_concentration
        }