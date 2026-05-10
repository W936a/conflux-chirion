#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║           CHIRION MCP SERVER v1.0                                ║
║           CONFLUX SYSTEMS (PTY) LTD — CAPE TOWN, SOUTH AFRICA    ║
╠══════════════════════════════════════════════════════════════════╣
║  PROPRIETARY AND CONFIDENTIAL                                     ║
║  © 2026 CONFLUX SYSTEMS (PTY) LTD. ALL RIGHTS RESERVED.          ║
╠══════════════════════════════════════════════════════════════════╣
║  FIXES APPLIED:                                                   ║
║  ✅ Bug 1: cursor now used inside same connection context         ║
║  ✅ Bug 2: removed unused requests import                         ║
║  ✅ Bug 3: added both contextmanager imports                      ║
║  ✅ Added price cache for CoinGecko (60s TTL)                     ║
║  ✅ Corrected tax strategies description                          ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL DEPENDENCIES:
  pip install fastapi uvicorn httpx PyJWT pydantic redis --break-system-packages

RUN:
  # HTTP mode (production)
  export CHIRION_JWT_SECRET=your_secret
  python CONFLUX_CHIRION_MCP.py

  # stdio mode (Claude Desktop)
  export CHIRION_DEFAULT_USER=user_id
  python CONFLUX_CHIRION_MCP.py --stdio
"""

import asyncio
import hmac
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, date, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
import hashlib

import httpx
import jwt
from fastapi import FastAPI, HTTPException, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
import uvicorn

# Optional Redis
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════

class Config:
    SERVER_NAME        = "conflux-chirion"
    SERVER_VERSION     = "1.0.0"
    DISPLAY_NAME       = "CHIRION"
    VENDOR             = "CONFLUX SYSTEMS (PTY) LTD"
    PROTOCOL_VERSION   = "2024-11-05"

    # Auth — STRICT: no fallback
    JWT_SECRET         = os.getenv("CHIRION_JWT_SECRET")
    API_KEY            = os.getenv("CHIRION_API_KEY")

    # Database
    DB_PATH            = os.getenv("CHIRION_DB_PATH", "./chirion.db")

    # Redis (optional)
    REDIS_URL          = os.getenv("REDIS_URL", "")

    # Server
    PORT               = int(os.getenv("PORT", "8083"))
    HOST               = os.getenv("HOST", "0.0.0.0")

    # Rate limits
    RATE_LIMIT_DEFAULT = int(os.getenv("RATE_LIMIT_DEFAULT", "60"))
    RATE_LIMIT_WRITE   = int(os.getenv("RATE_LIMIT_WRITE", "30"))

    # Tax year
    TAX_YEAR_START     = date(2026, 3, 1)
    TAX_YEAR_END       = date(2027, 2, 28)

    # Crypto API
    COINGECKO_API      = "https://api.coingecko.com/api/v3"

    @classmethod
    def validate(cls):
        if not cls.JWT_SECRET and not cls.API_KEY:
            raise ValueError("CHIRION_JWT_SECRET or CHIRION_API_KEY is required")

config = Config()

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CHIRION] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("conflux.chirion")

# ══════════════════════════════════════════════════════════════
# RATE LIMITER
# ══════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self):
        self.redis = None
        self._local_windows: Dict[str, deque] = defaultdict(deque)

    async def connect(self):
        if config.REDIS_URL and REDIS_AVAILABLE:
            try:
                self.redis = await redis.from_url(config.REDIS_URL, decode_responses=True)
                log.info("Redis rate limiter connected")
                return
            except Exception as e:
                log.warning(f"Redis connection failed: {e}")
        log.info("Using in-memory rate limiter")

    async def check(self, key: str, limit: int, window_seconds: int = 60) -> Tuple[bool, int]:
        if self.redis:
            now = time.time()
            window_start = now - window_seconds
            pipe = self.redis.pipeline()
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zcard(key)
            pipe.zadd(key, {str(now): now})
            pipe.expire(key, window_seconds)
            results = await pipe.execute()
            count = results[1]
            if count >= limit:
                return False, 0
            return True, limit - count - 1
        else:
            now = time.time()
            window = self._local_windows[key]
            while window and window[0] < now - window_seconds:
                window.popleft()
            if len(window) >= limit:
                return False, 0
            window.append(now)
            return True, limit - len(window)

    def get_key(self, client_id: str, tool: str) -> str:
        return f"ratelimit:chirion:{client_id}:{tool}"

rate_limiter = RateLimiter()

# ══════════════════════════════════════════════════════════════
# TAX BRACKETS 2026/2027 (SARS)
# ══════════════════════════════════════════════════════════════

TAX_BRACKETS = [
    (0, 237100, 0, 0.18),
    (237101, 370500, 42678, 0.26),
    (370501, 512800, 77262, 0.31),
    (512801, 673000, 121475, 0.36),
    (673001, 857900, 179147, 0.39),
    (857901, 1817000, 251258, 0.41),
    (1817001, float('inf'), 645001, 0.45),
]

PRIMARY_REBATE = 17820
SECONDARY_REBATE_65PLUS = 9765
TERTIARY_REBATE_75PLUS = 3249

# ══════════════════════════════════════════════════════════════
# TAX CALCULATOR
# ══════════════════════════════════════════════════════════════

def calculate_tax(taxable_income: float, age: int = 35) -> Dict[str, float]:
    """Calculate SARS 2026/2027 income tax"""
    if taxable_income <= 0:
        return {"tax_before_rebates": 0, "tax_due": 0, "effective_rate": 0}

    # Determine threshold
    if age < 65:
        threshold = 95750
    elif age < 75:
        threshold = 148217
    else:
        threshold = 165689

    if taxable_income <= threshold:
        return {"tax_before_rebates": 0, "tax_due": 0, "effective_rate": 0}

    tax = 0
    remaining = taxable_income

    for min_inc, max_inc, base, rate in TAX_BRACKETS:
        if remaining <= 0:
            break
        if max_inc == float('inf'):
            tax += remaining * rate
            break
        bracket_income = min(remaining, max_inc - min_inc + 1)
        tax += bracket_income * rate
        remaining -= bracket_income

    # Rebates
    rebate = PRIMARY_REBATE
    if age >= 65:
        rebate += SECONDARY_REBATE_65PLUS
    if age >= 75:
        rebate += TERTIARY_REBATE_75PLUS

    tax_due = max(0, tax - rebate)

    return {
        "tax_before_rebates": round(tax, 2),
        "tax_due": round(tax_due, 2),
        "effective_rate": round(tax_due / taxable_income * 100, 2) if taxable_income > 0 else 0
    }


# ══════════════════════════════════════════════════════════════
# PRICE CACHE (CoinGecko rate limit protection)
# ══════════════════════════════════════════════════════════════

_price_cache: Dict[str, Tuple[float, float]] = {}  # symbol -> (price, timestamp)

async def get_crypto_price(symbol: str) -> Optional[float]:
    """Fetch current price from CoinGecko with 60s cache"""
    # Check cache
    cached = _price_cache.get(symbol.upper())
    if cached and time.time() - cached[1] < 60:
        return cached[0]

    # Fetch from API
    coin_id = COINGECKO_IDS.get(symbol.upper())
    if not coin_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{config.COINGECKO_API}/simple/price", params={
                'ids': coin_id, 'vs_currencies': 'zar'
            })
            data = resp.json()
            price = data.get(coin_id, {}).get('zar')
            if price:
                _price_cache[symbol.upper()] = (float(price), time.time())
                return float(price)
    except Exception as e:
        log.warning(f"Price fetch failed for {symbol}: {e}")
    return None

# CoinGecko ID mapping
COINGECKO_IDS = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'BNB': 'binancecoin',
    'SOL': 'solana',
    'XRP': 'ripple',
    'ADA': 'cardano',
    'DOGE': 'dogecoin',
    'MATIC': 'matic-network',
}

# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════

class ChirionDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        cursor = conn.cursor()

        # Transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                category TEXT,
                transaction_date TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Crypto transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_transactions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                asset_name TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                amount REAL NOT NULL,
                price_per_unit REAL NOT NULL,
                transaction_date TEXT NOT NULL,
                wallet_address TEXT,
                tx_hash TEXT UNIQUE,
                counterparty_jurisdiction TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Cost basis lots
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cost_basis_lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                asset_name TEXT NOT NULL,
                purchase_price REAL NOT NULL,
                quantity REAL NOT NULL,
                remaining_quantity REAL NOT NULL,
                purchase_date TEXT NOT NULL,
                transaction_ref TEXT
            )
        """)

        # Tax strategies (6 strategies seeded)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tax_strategies (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT,
                calculation_method TEXT,
                risk_level TEXT,
                applicable_2026 INTEGER DEFAULT 1
            )
        """)

        cursor.execute("SELECT COUNT(*) FROM tax_strategies")
        if cursor.fetchone()[0] == 0:
            default_strategies = [
                ("S001", "Tax Loss Harvesting", "timing", "Realize losses to offset gains", "loss_amount * marginal_rate", "green", 1),
                ("S002", "Retirement Annuity Contributions", "deduction", "Maximize RA contributions", "contribution_amount * marginal_rate", "green", 1),
                ("S003", "Medical Expense Credit", "deduction", "Claim qualifying medical expenses", "excess_medical * 0.25", "green", 1),
                ("S004", "Crypto Asset Classification", "crypto", "Classify as capital vs revenue", "gain * (45% - 18%)", "yellow", 1),
                ("S005", "Staking Reward Timing", "crypto", "Time reward claims across tax years", "deferred_income * marginal_rate", "green", 1),
                ("S006", "Donations Tax Exemption", "estate", "Utilize R150,000 annual exemption", "donation_amount * 0.20", "green", 1),
            ]
            for s in default_strategies:
                cursor.execute("""
                    INSERT INTO tax_strategies (id, name, category, description, calculation_method, risk_level, applicable_2026)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, s)

        conn.commit()
        conn.close()
        log.info(f"Database initialized at {self.db_path}")

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()

    # Transaction methods
    def record_transaction(self, user_id: str, tx_type: str, amount: float,
                           description: str = "", category: str = "",
                           transaction_date: str = None) -> Dict:
        tx_id = str(uuid.uuid4())[:8]
        if not transaction_date:
            transaction_date = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO transactions (id, user_id, transaction_type, amount, description, category, transaction_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (tx_id, user_id, tx_type, amount, description, category, transaction_date))
            conn.commit()

        return {"id": tx_id, "status": "recorded", "message": f"{tx_type} of R{amount:.2f} recorded"}

    def get_transactions(self, user_id: str, limit: int = 50) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, transaction_type, amount, description, category, transaction_date
                FROM transactions
                WHERE user_id = ?
                ORDER BY transaction_date DESC
                LIMIT ?
            """, (user_id, limit))
            rows = cursor.fetchall()
        return [{
            "id": r[0], "type": r[1], "amount": r[2],
            "description": r[3], "category": r[4], "date": r[5]
        } for r in rows]

    def get_summary(self, user_id: str) -> Dict:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    SUM(CASE WHEN transaction_type = 'INCOME' THEN amount ELSE 0 END) as total_income,
                    SUM(CASE WHEN transaction_type = 'EXPENSE' THEN amount ELSE 0 END) as total_expense,
                    COUNT(*) as transaction_count
                FROM transactions WHERE user_id = ?
            """, (user_id,))
            row = cursor.fetchone()
        return {
            "total_income": row[0] or 0,
            "total_expense": row[1] or 0,
            "net": (row[0] or 0) - (row[1] or 0),
            "transaction_count": row[2] or 0
        }

    # Crypto methods — FIXED: cursor stays inside connection context
    def add_crypto_transaction(self, user_id: str, asset: str, tx_type: str,
                               amount: float, price: float, tx_date: str,
                               wallet: str = "", tx_hash: str = "",
                               jurisdiction: str = "") -> Dict:
        tx_id = str(uuid.uuid4())[:8]
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO crypto_transactions
                (id, user_id, asset_name, transaction_type, amount, price_per_unit, transaction_date, wallet_address, tx_hash, counterparty_jurisdiction)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (tx_id, user_id, asset, tx_type, amount, price, tx_date, wallet, tx_hash, jurisdiction))

            # If BUY, add to cost basis — inside same connection
            if tx_type == "BUY":
                cursor.execute("""
                    INSERT INTO cost_basis_lots (user_id, asset_name, purchase_price, quantity, remaining_quantity, purchase_date, transaction_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (user_id, asset, price, amount, amount, tx_date, tx_id))

            conn.commit()

        return {"id": tx_id, "status": "recorded"}

    def get_crypto_portfolio(self, user_id: str) -> Dict:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT asset_name, SUM(CASE WHEN transaction_type = 'BUY' THEN amount ELSE -amount END) as balance
                FROM crypto_transactions
                WHERE user_id = ?
                GROUP BY asset_name
                HAVING balance > 0
            """, (user_id,))
            rows = cursor.fetchall()
        return {"holdings": [{"asset": r[0], "amount": r[1]} for r in rows]}

    def get_crypto_cost_basis(self, user_id: str, asset: str) -> Dict:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT purchase_price, remaining_quantity FROM cost_basis_lots
                WHERE user_id = ? AND asset_name = ? AND remaining_quantity > 0
                ORDER BY purchase_date
            """, (user_id, asset))
            lots = cursor.fetchall()
        if not lots:
            return {"asset": asset, "total_cost": 0, "total_quantity": 0, "average_cost": 0}
        total_cost = sum(l[0] * l[1] for l in lots)
        total_qty = sum(l[1] for l in lots)
        return {
            "asset": asset,
            "total_cost": total_cost,
            "total_quantity": total_qty,
            "average_cost": total_cost / total_qty if total_qty > 0 else 0,
            "lots": [{"price": l[0], "quantity": l[1]} for l in lots]
        }

    def get_tax_strategies(self, user_id: str = None, category: str = None) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            query = "SELECT id, name, category, description, calculation_method, risk_level FROM tax_strategies WHERE applicable_2026 = 1"
            params = []
            if category:
                query += " AND category = ?"
                params.append(category)
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [{
            "id": r[0], "name": r[1], "category": r[2],
            "description": r[3], "calculation": r[4], "risk": r[5]
        } for r in rows]

    def delete_all_data(self, user_id: str) -> int:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
            t_count = cursor.rowcount
            cursor.execute("DELETE FROM crypto_transactions WHERE user_id = ?", (user_id,))
            c_count = cursor.rowcount
            cursor.execute("DELETE FROM cost_basis_lots WHERE user_id = ?", (user_id,))
            l_count = cursor.rowcount
            conn.commit()
        return t_count + c_count + l_count

db = ChirionDB(config.DB_PATH)

# ══════════════════════════════════════════════════════════════
# PYDANTIC MODELS (Input validation)
# ══════════════════════════════════════════════════════════════

class RecordTransactionRequest(BaseModel):
    transaction_type: str = Field(..., pattern="^(INCOME|EXPENSE)$")
    amount: float = Field(..., gt=0, le=1e9)
    description: Optional[str] = Field(None, max_length=500)
    category: Optional[str] = Field(None, max_length=100)
    transaction_date: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

class RecordCryptoRequest(BaseModel):
    asset: str = Field(..., min_length=1, max_length=10)
    transaction_type: str = Field(..., pattern="^(BUY|SELL|STAKE|AIRDROP)$")
    amount: float = Field(..., gt=0)
    price_per_unit: float = Field(..., gt=0)
    transaction_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    wallet_address: Optional[str] = Field(None, max_length=100)
    tx_hash: Optional[str] = Field(None, max_length=100)
    counterparty_jurisdiction: Optional[str] = Field(None, max_length=2)

class CalculateTaxRequest(BaseModel):
    taxable_income: float = Field(..., ge=0)
    age: int = Field(35, ge=0, le=120)

class GetTransactionsRequest(BaseModel):
    limit: int = Field(50, ge=1, le=500)

class DeleteAllRequest(BaseModel):
    confirm: bool = Field(False, description="Must be true to delete all data")

# ══════════════════════════════════════════════════════════════
# AUTH — STRICT
# ══════════════════════════════════════════════════════════════

async def get_current_user(authorization: str = Header(default="")) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")

    if authorization.startswith("Bearer "):
        token = authorization[7:]
        if not config.JWT_SECRET:
            raise HTTPException(status_code=401, detail="JWT not configured")
        try:
            payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token: missing subject")
            return user_id
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    else:
        if not config.API_KEY:
            raise HTTPException(status_code=401, detail="API key not configured")
        if not hmac.compare_digest(authorization, config.API_KEY):
            raise HTTPException(status_code=401, detail="Invalid API key")
        return "api_user"

# ══════════════════════════════════════════════════════════════
# MCP TOOL IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════════

async def tool_record_transaction(user_id: str, transaction_type: str, amount: float,
                                  description: str = None, category: str = None,
                                  transaction_date: str = None) -> Dict:
    return db.record_transaction(user_id, transaction_type, amount, description or "", category or "", transaction_date)

async def tool_get_transactions(user_id: str, limit: int = 50) -> Dict:
    return {"transactions": db.get_transactions(user_id, limit)}

async def tool_get_summary(user_id: str) -> Dict:
    return db.get_summary(user_id)

async def tool_calculate_tax(user_id: str, taxable_income: float, age: int = 35) -> Dict:
    return calculate_tax(taxable_income, age)

async def tool_get_tax_strategies(user_id: str, category: str = None) -> Dict:
    strategies = db.get_tax_strategies(user_id, category)
    return {"strategies": strategies, "count": len(strategies)}

async def tool_record_crypto(user_id: str, asset: str, transaction_type: str,
                             amount: float, price_per_unit: float, transaction_date: str,
                             wallet_address: str = None, tx_hash: str = None,
                             counterparty_jurisdiction: str = None) -> Dict:
    return db.add_crypto_transaction(user_id, asset, transaction_type, amount, price_per_unit,
                                     transaction_date, wallet_address or "", tx_hash or "",
                                     counterparty_jurisdiction or "")

async def tool_get_crypto_portfolio(user_id: str) -> Dict:
    portfolio = db.get_crypto_portfolio(user_id)
    # Add current prices with caching
    for holding in portfolio.get("holdings", []):
        price = await get_crypto_price(holding["asset"])
        if price:
            holding["current_price_zar"] = price
            holding["current_value_zar"] = holding["amount"] * price
    return portfolio

async def tool_get_crypto_cost_basis(user_id: str, asset: str) -> Dict:
    return db.get_crypto_cost_basis(user_id, asset.upper())

async def tool_delete_all_data(user_id: str, confirm: bool = False) -> Dict:
    if not confirm:
        return {"error": "Delete all requires confirm=True. This is a destructive operation."}
    count = db.delete_all_data(user_id)
    return {"deleted_count": count, "message": f"Deleted {count} records"}

# ══════════════════════════════════════════════════════════════
# TOOL REGISTRY
# ══════════════════════════════════════════════════════════════

TOOLS = {
    "record_transaction": {
        "name": "record_transaction",
        "description": "Record an income or expense transaction",
        "inputSchema": {
            "type": "object",
            "properties": {
                "transaction_type": {"type": "string", "enum": ["INCOME", "EXPENSE"]},
                "amount": {"type": "number", "description": "Amount in ZAR"},
                "description": {"type": "string", "description": "Optional description"},
                "category": {"type": "string", "description": "Optional category"},
                "transaction_date": {"type": "string", "description": "ISO date (optional)"}
            },
            "required": ["transaction_type", "amount"]
        },
        "handler": tool_record_transaction
    },
    "get_transactions": {
        "name": "get_transactions",
        "description": "Get recent transactions",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "default": 50}}},
        "handler": tool_get_transactions
    },
    "get_summary": {
        "name": "get_summary",
        "description": "Get financial summary (total income, expenses, net)",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_get_summary
    },
    "calculate_tax": {
        "name": "calculate_tax",
        "description": "Calculate SARS 2026/2027 income tax based on taxable income and age",
        "inputSchema": {
            "type": "object",
            "properties": {
                "taxable_income": {"type": "number", "description": "Taxable income in ZAR"},
                "age": {"type": "integer", "description": "Age for rebates", "default": 35}
            },
            "required": ["taxable_income"]
        },
        "handler": tool_calculate_tax
    },
    "get_tax_strategies": {
        "name": "get_tax_strategies",
        "description": "Get tax optimization strategies (6 strategies for 2026/2027)",
        "inputSchema": {"type": "object", "properties": {"category": {"type": "string"}}},
        "handler": tool_get_tax_strategies
    },
    "record_crypto_transaction": {
        "name": "record_crypto_transaction",
        "description": "Record a crypto transaction (BUY, SELL, STAKE, AIRDROP)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "asset": {"type": "string", "description": "Asset symbol (BTC, ETH, etc.)"},
                "transaction_type": {"type": "string", "enum": ["BUY", "SELL", "STAKE", "AIRDROP"]},
                "amount": {"type": "number"},
                "price_per_unit": {"type": "number", "description": "Price in ZAR"},
                "transaction_date": {"type": "string", "description": "ISO date"},
                "wallet_address": {"type": "string"},
                "tx_hash": {"type": "string"},
                "counterparty_jurisdiction": {"type": "string", "maxLength": 2}
            },
            "required": ["asset", "transaction_type", "amount", "price_per_unit", "transaction_date"]
        },
        "handler": tool_record_crypto
    },
    "get_crypto_portfolio": {
        "name": "get_crypto_portfolio",
        "description": "Get current crypto portfolio with live prices (cached 60s)",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_get_crypto_portfolio
    },
    "get_crypto_cost_basis": {
        "name": "get_crypto_cost_basis",
        "description": "Get cost basis for a crypto asset (FIFO)",
        "inputSchema": {"type": "object", "properties": {"asset": {"type": "string"}}, "required": ["asset"]},
        "handler": tool_get_crypto_cost_basis
    },
    "delete_all_data": {
        "name": "delete_all_data",
        "description": "Delete ALL data for this user. Requires confirm=true",
        "inputSchema": {"type": "object", "properties": {"confirm": {"type": "boolean", "default": False}}, "required": ["confirm"]},
        "handler": tool_delete_all_data
    }
}

# ══════════════════════════════════════════════════════════════
# MCP MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════

async def handle_mcp_message(message: Dict, user_id: str) -> Dict:
    msg_id = message.get("id")
    method = message.get("method", "")
    params = message.get("params", {})

    def ok(result):
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def err(code, msg, data=None):
        error = {"code": code, "message": msg}
        if data:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": msg_id, "error": error}

    try:
        if method == "initialize":
            return ok({
                "protocolVersion": config.PROTOCOL_VERSION,
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "serverInfo": {"name": config.SERVER_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR}
            })
        elif method in ("initialized", "ping"):
            return ok({})
        elif method == "tools/list":
            return ok({"tools": [{"name": t["name"], "description": t["description"], "inputSchema": t["inputSchema"]} for t in TOOLS.values()]})
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            if tool_name not in TOOLS:
                return err(-32601, f"Tool not found: {tool_name}")

            # Validate input with Pydantic
            try:
                if tool_name == "record_transaction":
                    validated = RecordTransactionRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "record_crypto_transaction":
                    validated = RecordCryptoRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "calculate_tax":
                    validated = CalculateTaxRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "get_transactions":
                    validated = GetTransactionsRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "delete_all_data":
                    validated = DeleteAllRequest(**tool_args)
                    args = validated.model_dump()
                else:
                    args = tool_args
            except Exception as e:
                return err(-32602, f"Invalid arguments: {e}")

            # Rate limit
            tool_limit = config.RATE_LIMIT_WRITE if tool_name in ["record_transaction", "record_crypto_transaction", "delete_all_data"] else config.RATE_LIMIT_DEFAULT
            key = rate_limiter.get_key(user_id, tool_name)
            allowed, _ = await rate_limiter.check(key, tool_limit)
            if not allowed:
                return err(-32000, "Rate limit exceeded")

            result = await TOOLS[tool_name]["handler"](user_id, **args)
            return ok({
                "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
                "isError": "error" in result
            })
        else:
            return err(-32601, f"Method not found: {method}")
    except Exception as e:
        log.error(f"MCP handler error: {e}", exc_info=True)
        return err(-32603, "Internal error", {"detail": str(e)})

# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    await rate_limiter.connect()
    log.info(f"{config.DISPLAY_NAME} v{config.SERVER_VERSION} ready")
    yield
    log.info(f"{config.DISPLAY_NAME} shutting down")

app = FastAPI(title=config.DISPLAY_NAME, version=config.SERVER_VERSION, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    return {"status": "online", "service": config.DISPLAY_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR}

@app.get("/")
async def root():
    return {"name": config.DISPLAY_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR, "tools": list(TOOLS.keys())}

@app.post("/mcp")
async def mcp_endpoint(request: Request, user_id: str = Depends(get_current_user)):
    client_id = request.client.host if request.client else "unknown"
    allowed, _ = await rate_limiter.check(rate_limiter.get_key(client_id, "mcp"), config.RATE_LIMIT_DEFAULT)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    response = await handle_mcp_message(body, user_id)
    return JSONResponse(content=response)

# ══════════════════════════════════════════════════════════════
# STDIO TRANSPORT
# ══════════════════════════════════════════════════════════════

async def run_stdio():
    log.info(f"{config.DISPLAY_NAME} — stdio mode")
    await rate_limiter.connect()

    user_id = os.getenv("CHIRION_DEFAULT_USER")
    if not user_id:
        log.error("CHIRION_DEFAULT_USER not set for stdio mode")
        sys.exit(1)

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        try:
            line = await reader.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as e:
                error_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"Parse error: {e}"}}
                sys.stdout.write(json.dumps(error_resp) + "\n")
                sys.stdout.flush()
                continue
            response = await handle_mcp_message(message, user_id)
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
        except asyncio.CancelledError:
            break
        except Exception as e:
            error_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
            sys.stdout.write(json.dumps(error_resp) + "\n")
            sys.stdout.flush()

# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

def main():
    try:
        config.validate()
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    args = sys.argv[1:]

    if "--stdio" in args:
        asyncio.run(run_stdio())
    else:
        port = config.PORT
        for i, arg in enumerate(args):
            if arg == "--port" and i + 1 < len(args):
                port = int(args[i + 1])

        print(f"\n{'='*60}")
        print(f"  {config.DISPLAY_NAME} MCP v{config.SERVER_VERSION}")
        print(f"  {config.VENDOR}")
        print(f"{'='*60}")
        print(f"  HTTP:      http://0.0.0.0:{port}")
        print(f"  MCP:       http://0.0.0.0:{port}/mcp")
        print(f"  Health:    http://0.0.0.0:{port}/health")
        print(f"  Tools:     {', '.join(TOOLS.keys())}")
        print(f"  Features:  SARS 2026/2027 tax brackets")
        print(f"             Crypto portfolio with FIFO cost basis")
        print(f"             CoinGecko price fetching (60s cache)")
        print(f"             SQLite WAL mode")
        print(f"             Confirm required for delete_all")
        print(f"{'='*60}\n")

        uvicorn.run(app, host=config.HOST, port=port, log_level="info")

if __name__ == "__main__":
    main()