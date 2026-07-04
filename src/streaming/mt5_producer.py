import os
import time
import json
from datetime import datetime
from collections import deque

import MetaTrader5 as mt5
import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_XAUUSD", "xauusd_raw")
MT5_LOGIN = int(os.getenv("MT5_LOGIN", "123456"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "yourpass")
MT5_SERVER = os.getenv("MT5_SERVER", "ICMarkets-Demo")
MT5_SYMBOL = os.getenv("MT5_SYMBOL", "XAUUSD")
POLL_INTERVAL = float(os.getenv("MT5_POLL_INTERVAL", "1"))
VOLATILITY_WINDOW = 60

price_history = deque(maxlen=VOLATILITY_WINDOW)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def connect_mt5():
    if not mt5.initialize():
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")

    account_info = mt5.account_info()
    if account_info is not None:
        print(f"Already connected | Account: {account_info.login}")
    else:
        authorized = mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
        if not authorized:
            raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")
        print(f"Connected to MT5 | Account: {mt5.account_info().login}")


def calculate_volatility(values: deque):
    if len(values) < 2:
        return None
    return round(pd.Series(list(values)).std(), 5)


def stream_ticks():
    if not mt5.symbol_select(MT5_SYMBOL, True):
        raise RuntimeError(f"Symbol {MT5_SYMBOL} not found")

    print(f"Streaming {MT5_SYMBOL} to Kafka topic '{KAFKA_TOPIC}'...")
    prev_mid = None

    while True:
        tick = mt5.symbol_info_tick(MT5_SYMBOL)
        if tick:
            mid = round((tick.bid + tick.ask) / 2, 5)
            price_change = round(mid - prev_mid, 5) if prev_mid is not None else 0.0
            price_history.append(mid)
            volatility = calculate_volatility(price_history)

            payload = {
                "symbol": MT5_SYMBOL,
                "timestamp": datetime.utcfromtimestamp(tick.time).isoformat(),
                "bid": tick.bid,
                "ask": tick.ask,
                "mid": mid,
                "spread": round(tick.ask - tick.bid, 5),
                "last_price": tick.last if tick.last > 0 else mid,
                "price_change": price_change,
                "volatility": volatility,
                "volume": tick.volume,
                "source": "MT5",
            }

            producer.send(KAFKA_TOPIC, value=payload)
            print(
                f"[{payload['timestamp']}] "
                f"BID={tick.bid} | ASK={tick.ask} | "
                f"SPREAD={payload['spread']} | Δ={price_change:+.5f}"
            )
            prev_mid = mid

            if prev_mid and mid > 0 and abs(price_change / mid * 100) > 2.0:
                from scripts.telegram_alert import send_business_xauusd_spike
                send_business_xauusd_spike(MT5_SYMBOL, price_change, 2.0)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    connect_mt5()
    stream_ticks()
