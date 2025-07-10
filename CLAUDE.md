# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Backpack Exchange arbitrage trading system focused on spot-futures arbitrage strategies. The project integrates with the Backpack Exchange API to identify and execute arbitrage opportunities between spot and perpetual futures markets.

## Development Environment

- **Python Version**: >=3.12 (specified in pyproject.toml)
- **Package Manager**: Uses standard Python tooling with pyproject.toml
- **Dependencies**: Minimal setup, add trading/API dependencies as needed

## Key API Integration Points

The system integrates with Backpack Exchange API which requires:

### Authentication
- ED25519 keypair for signing requests
- Headers: `X-Timestamp`, `X-Window`, `X-API-Key`, `X-Signature`
- Instruction-based signing for different operations (orderExecute, balanceQuery, etc.)

### Core Endpoints for Arbitrage
- `/balances` - Account balance checking (instruction: balanceQuery)
- `/orders/execute` - Order placement (instruction: orderExecute)
- `/orders` - Order management and history (instruction: orderQueryAll)
- `/positions` - Position tracking (instruction: positionQuery)
- `/markets` - Market data and pricing (public endpoint)
- `/depth` - Order book data for price discovery
- `/fills` - Trade history and execution tracking (instruction: fillHistoryQueryAll)

### WebSocket Streams
- Real-time market data: `depth.<symbol>`, `ticker.<symbol>`
- Private streams: `account.orderUpdate.<symbol>`, `account.positionUpdate.<symbol>`
- Authentication required for private streams with subscribe instruction

## Architecture Notes

### Arbitrage Strategy Components
1. **Market Data Collection**: Real-time spot and futures price monitoring
2. **Spread Analysis**: Calculate funding rates and price differences
3. **Risk Management**: Position sizing and exposure limits
4. **Order Execution**: Simultaneous spot/futures order placement
5. **Position Monitoring**: Track P&L and funding payments

### Key Market Types
- Spot markets (e.g., SOL_USDC)
- Perpetual futures (e.g., SOL_USDC_PERP)
- Funding rate arbitrage opportunities

## Development Commands

```bash
# Run the application
uv run main.py

# Run any Python script
uv run script.py

# Install dependencies (add as needed)
uv add package_name

# Development server (add when implementing web interface)
# uv run uvicorn main:app --reload
```

## File Structure Guidelines

- `hello.py` - Current entry point (to be replaced with main arbitrage logic)
- `openapi.yaml` - Complete Backpack Exchange API specification
- `pyproject.toml` - Python project configuration
- Future additions:
  - `src/` - Main application code
  - `src/api/` - Backpack API client and authentication
  - `src/strategy/` - Arbitrage strategy implementation
  - `src/data/` - Market data handling and WebSocket connections
  - `src/risk/` - Risk management and position sizing

## Important API Considerations

- All private API calls must be signed with ED25519 keypairs
- Rate limiting applies - implement proper request throttling
- WebSocket connections require ping/pong for keepalive (60s intervals)
- Order execution supports batch operations for simultaneous trades
- Funding payments occur every 8 hours on perpetual contracts
- Use reduce-only orders for position closure to prevent over-exposure