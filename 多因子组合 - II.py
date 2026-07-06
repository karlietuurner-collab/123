#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎯 BTC StochRSI + ADX + 订单流因子 - 5分钟周期 + 提前1分钟预测
----------------------------------------------------------
- 双WebSocket：K线流 + 逐笔交易流
- StochRSI为主信号，ADX调节严格度，订单流因子作为确认/备用
- 固定终端布局：标题、统计面板、实时信息行、历史表格（20条）
- 局部刷新，避免刷屏
----------------------------------------------------------
"""

import asyncio
import websockets
import json
import numpy as np
from datetime import datetime, timedelta, timezone
import sys
import os
import requests
import time
import talib
import calendar
from collections import deque
from colorama import init, Fore, Style

# Windows 终端支持
if sys.platform == "win32":
    os.system('color')
    import ctypes
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.GetStdHandle(-11)
    mode = ctypes.c_ulong()
    kernel32.GetConsoleMode(handle, ctypes.byref(mode))
    mode.value |= 0x0004
    kernel32.SetConsoleMode(handle, mode)
    os.system('color 0F')

# 强制刷新输出
import builtins
original_print = builtins.print
def print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    original_print(*args, **kwargs)
builtins.print = print

init(autoreset=True, convert=True, strip=False)

# ================== 配置 ==================
CONFIG = {
    'kline_url': 'wss://stream.binance.com:443/ws/btcusdt@kline_5m',
    'trade_url': 'wss://stream.binance.com:443/ws/btcusdt@trade',
    'rest_api': 'https://api.binance.com',
    'rest_api_backup': 'https://api1.binance.com',
    
    # StochRSI 参数
    'rsi_period': 14,
    'stoch_period': 14,
    'k_smooth': 2,
    'd_smooth': 3,
    'cross_threshold': 3.0,
    
    # ADX 参数
    'adx_period': 14,
    'adx_trend_threshold': 25,
    'adx_choppy_threshold': 20,
    
    # 订单流因子阈值
    'net_buy_ratio_high': 0.5,
    'net_buy_ratio_medium': 0.25,
    'net_buy_ratio_low': -0.25,
    'close_position_up': 0.8,
    'close_position_down': 0.2,
    'avg_buy_price_ratio_up': 1.02,
    'avg_buy_price_ratio_down': 0.98,
    'sell_ratio_up': 0.35,
    'sell_ratio_down': 0.65,
    'volume_shrink_threshold': 0.7,
    'amplitude_percentile': 80,
    
    # 因子投票权重
    'factor_weights': {
        'net_buy_high': 2.0,
        'net_buy_medium': 1.5,
        'net_buy_low': -1.5,
        'close_pos_up': 1.0,
        'close_pos_down': -1.0,
        'avg_buy_up': 1.0,
        'avg_buy_down': -1.0,
        'sell_ratio_up': 1.0,
        'sell_ratio_down': -1.0,
        'volume_shrink': -1.0,
        'amplitude_trend': 0.5,
    },
    
    # 融合参数
    'min_active_factors': 2,
    'consensus_ratio': 0.5,
    'backup_strength_threshold': 1.5,
    'adx_trend_loosen': True,
    'adx_choppy_tighten': True,
    
    # 历史限制
    'history_limit': 10000,
    'early_prediction_seconds': 60,
    
    # API 配置
    'api_key': 'RcRDAx6bMXYK3DEjP3rKPWZXjPxXDyldJ2Oim1DlA80iMCMOkhtUDGnJ3mAO85c5',
    'api_secret': 'vVP7VDBNdrsVy6rS6UKaCtwtSrd2BHxO5yzv6T3VkSS7kPtchGUTCDVjdMTs7W71'
}

# ================== 时间工具 ==================
def now_utc():
    """获取当前 UTC 时间 (naive datetime)"""
    return datetime.utcnow()

def timestamp_to_utc_str(ts_ms, format_str='%H:%M:%S'):
    if ts_ms is None:
        return '--:--:--'
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    return dt.strftime(format_str)

def datetime_to_timestamp(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return int(calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond / 1000)
    else:
        return int(dt.timestamp() * 1000)

def get_kline_natural_start_time(timeframe, base_time=None):
    if base_time is None:
        base_time = now_utc()
    if timeframe == '5m':
        m = base_time.minute - (base_time.minute % 5)
        return base_time.replace(minute=m, second=0, microsecond=0)
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

def get_last_closed_kline_end_time(timeframe='5m'):
    now = now_utc()
    current_start = get_kline_natural_start_time(timeframe, now)
    return datetime_to_timestamp(current_start)

# ================== 技术指标类 ==================
class TechnicalIndicators:
    def __init__(self, timestamp: datetime, stoch_rsi_k: float, stoch_rsi_d: float, 
                 adx: float, plus_di: float = None, minus_di: float = None):
        self.timestamp = timestamp
        self.stoch_rsi_k = stoch_rsi_k
        self.stoch_rsi_d = stoch_rsi_d
        self.adx = adx
        self.plus_di = plus_di
        self.minus_di = minus_di

    @property
    def stoch_golden_cross(self) -> bool:
        return (self.stoch_rsi_k > self.stoch_rsi_d and 
                (self.stoch_rsi_k - self.stoch_rsi_d) >= CONFIG['cross_threshold'])

    @property
    def stoch_death_cross(self) -> bool:
        return (self.stoch_rsi_k < self.stoch_rsi_d and 
                (self.stoch_rsi_d - self.stoch_rsi_k) >= CONFIG['cross_threshold'])

    @property
    def market_condition(self) -> str:
        if self.adx >= CONFIG['adx_trend_threshold']:
            return "TRENDING"
        elif self.adx <= CONFIG['adx_choppy_threshold']:
            return "CHOPPY"
        else:
            return "NEUTRAL"

    @property
    def trend_strength(self) -> str:
        if self.adx >= 50:
            return "VERY_STRONG"
        elif self.adx >= CONFIG['adx_trend_threshold']:
            return "STRONG" 
        elif self.adx >= CONFIG['adx_choppy_threshold']:
            return "MODERATE"
        else:
            return "WEAK"

class Kline5:
    def __init__(self, open_time: datetime, close_time: datetime,
                 open_price: float, high_price: float, low_price: float, 
                 close_price: float, indicators: TechnicalIndicators = None):
        self.open_time = open_time
        self.close_time = close_time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.indicators = indicators

    @property
    def direction(self) -> str:
        if self.close_price > self.open_price:
            return "UP"
        elif self.close_price < self.open_price:
            return "DOWN"
        return "FLAT"

# ================== 订单流因子计算器 ==================
class OrderFlowFactors:
    """管理订单流相关因子，包括从逐笔交易计算的数据以及从K线派生的因子"""
    def __init__(self, max_history=100):
        self.current_delta = 0.0
        self.current_buy_volume = 0.0
        self.current_sell_volume = 0.0
        self.current_total_volume = 0.0
        self.current_buy_value = 0.0
        self.current_sell_value = 0.0
        self.current_trade_count = 0
        
        self.current_kline_start_ms = None
        self.current_kline_end_ms = None
        self.historical_factors = deque(maxlen=max_history)
        
        self.lock = asyncio.Lock()
        self.close_prices = []
        self.high_prices = []
        self.low_prices = []
        self.volumes = []
        
    async def process_trade(self, trade_data):
        price = float(trade_data['p'])
        quantity = float(trade_data['q'])
        is_seller = trade_data['m']
        
        async with self.lock:
            self.current_trade_count += 1
            self.current_total_volume += quantity
            if is_seller:
                self.current_sell_volume += quantity
                self.current_sell_value += price * quantity
                self.current_delta -= quantity
            else:
                self.current_buy_volume += quantity
                self.current_buy_value += price * quantity
                self.current_delta += quantity
                
    async def reset_for_new_kline(self, kline_start_ms, kline_end_ms):
        async with self.lock:
            if self.current_kline_start_ms is not None:
                factors = self.calculate_factors_from_current()
                self.historical_factors.append(factors)
            self.current_delta = 0.0
            self.current_buy_volume = 0.0
            self.current_sell_volume = 0.0
            self.current_total_volume = 0.0
            self.current_buy_value = 0.0
            self.current_sell_value = 0.0
            self.current_trade_count = 0
            self.current_kline_start_ms = kline_start_ms
            self.current_kline_end_ms = kline_end_ms
            
    def calculate_factors_from_current(self):
        factors = {}
        if self.current_total_volume > 0:
            factors['net_buy_ratio'] = (self.current_buy_volume - self.current_sell_volume) / self.current_total_volume
            factors['buy_ratio'] = self.current_buy_volume / self.current_total_volume
            factors['sell_ratio'] = self.current_sell_volume / self.current_total_volume
            factors['avg_buy_price'] = self.current_buy_value / self.current_buy_volume if self.current_buy_volume > 0 else 0
            factors['avg_sell_price'] = self.current_sell_value / self.current_sell_volume if self.current_sell_volume > 0 else 0
        else:
            factors['net_buy_ratio'] = 0
            factors['buy_ratio'] = 0
            factors['sell_ratio'] = 0
            factors['avg_buy_price'] = 0
            factors['avg_sell_price'] = 0
        return factors
    
    async def get_current_factors(self, current_close, current_high, current_low):
        async with self.lock:
            factors = {}
            if self.current_total_volume > 0:
                factors['net_buy_ratio'] = (self.current_buy_volume - self.current_sell_volume) / self.current_total_volume
                factors['sell_ratio'] = self.current_sell_volume / self.current_total_volume
                factors['buy_ratio'] = self.current_buy_volume / self.current_total_volume
                factors['avg_buy_price'] = self.current_buy_value / self.current_buy_volume if self.current_buy_volume > 0 else current_close
                factors['avg_sell_price'] = self.current_sell_value / self.current_sell_volume if self.current_sell_volume > 0 else current_close
            else:
                factors['net_buy_ratio'] = 0
                factors['sell_ratio'] = 0
                factors['buy_ratio'] = 0
                factors['avg_buy_price'] = current_close
                factors['avg_sell_price'] = current_close
            
            if len(self.close_prices) >= 50:
                past_high = max(self.high_prices[-50:])
                past_low = min(self.low_prices[-50:])
                if past_high - past_low > 0:
                    factors['close_position'] = (current_close - past_low) / (past_high - past_low)
                else:
                    factors['close_position'] = 0.5
            else:
                factors['close_position'] = 0.5
            
            if factors['avg_buy_price'] > 0:
                factors['avg_buy_vs_close'] = factors['avg_buy_price'] / current_close
            else:
                factors['avg_buy_vs_close'] = 1.0
            
            if len(self.volumes) >= 20:
                avg_volume = np.mean(self.volumes[-20:])
                if avg_volume > 0:
                    factors['volume_shrink'] = self.current_total_volume / avg_volume
                else:
                    factors['volume_shrink'] = 1.0
            else:
                factors['volume_shrink'] = 1.0
            
            if current_high > current_low:
                factors['amplitude'] = (current_high - current_low) / current_low
            else:
                factors['amplitude'] = 0.0
                
            if len(self.historical_factors) >= 50:
                hist_amps = [f.get('amplitude', 0) for f in self.historical_factors if 'amplitude' in f]
                if hist_amps:
                    percentile = np.percentile(hist_amps, CONFIG['amplitude_percentile'])
                    factors['amplitude_high'] = factors['amplitude'] >= percentile
                else:
                    factors['amplitude_high'] = False
            else:
                factors['amplitude_high'] = False
            
            return factors
    
    def update_ohlcv(self, close, high, low, volume):
        self.close_prices.append(close)
        self.high_prices.append(high)
        self.low_prices.append(low)
        self.volumes.append(volume)
        max_len = 1000
        if len(self.close_prices) > max_len:
            self.close_prices = self.close_prices[-max_len:]
            self.high_prices = self.high_prices[-max_len:]
            self.low_prices = self.low_prices[-max_len:]
            self.volumes = self.volumes[-max_len:]

# ================== 技术指标计算器 ==================
class TechnicalCalculator:
    def __init__(self):
        self.rsi_period = CONFIG['rsi_period']
        self.stoch_period = CONFIG['stoch_period']
        self.k_smooth = CONFIG['k_smooth']
        self.d_smooth = CONFIG['d_smooth']
        self.adx_period = CONFIG['adx_period']

        self.close_prices = []
        self.high_prices = []
        self.low_prices = []
        self.volumes = []
        self.max_history = 3000

        self.last_indicators = None
        self.last_update_time = None
        self.initialized = False
        
        self.price_cache = {}
        self.last_api_call = 0

    def fetch_historical_klines(self):
        apis = [CONFIG['rest_api'], CONFIG['rest_api_backup']]
        headers = {'X-MBX-APIKEY': CONFIG['api_key']}
        needed_bars = CONFIG['history_limit'] + 300
        end_time = get_last_closed_kline_end_time('5m')
        all_klines = []
        current_end = end_time
        limit = 1000

        print(Fore.YELLOW + Style.BRIGHT + f"📡 开始分页获取 {needed_bars} 根历史数据...")

        while len(all_klines) < needed_bars:
            current_start = current_end - limit * 5 * 60 * 1000

            for api in apis:
                try:
                    url = f"{api}/api/v3/klines"
                    params = {
                        'symbol': 'BTCUSDT',
                        'interval': '5m',
                        'startTime': int(current_start),
                        'endTime': int(current_end),
                        'limit': limit
                    }
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    if len(data) > 0:
                        all_klines = data + all_klines
                        current_end = data[0][0] - 1
                        print(Fore.CYAN + f"  已获取 {len(all_klines)} 根...")
                        break
                except Exception as e:
                    print(Fore.RED + f"❌ {api} 分页获取失败: {e}")
                    continue
            else:
                print(Fore.RED + "❌ 所有 API 均无法获取历史数据")
                return False

            if len(data) == 0:
                break
            time.sleep(0.1)

        if len(all_klines) < 100:
            print(Fore.RED + f"❌ 数据不足，仅获取到 {len(all_klines)} 根")
            return False

        recent_klines = all_klines[-needed_bars:]
        self.close_prices = [float(k[4]) for k in recent_klines]
        self.high_prices = [float(k[2]) for k in recent_klines]
        self.low_prices = [float(k[3]) for k in recent_klines]
        self.volumes = [float(k[5]) for k in recent_klines]

        print(Fore.GREEN + f"✅ 共载入 {len(self.close_prices)} 根历史5m OHLCV数据")
        self.last_update_time = now_utc()
        self.initialized = True
        return True

    def confirm_kline_ohlc(self, symbol, close_time_ms):
        cache_key = f"{symbol}_{close_time_ms}"
        if cache_key in self.price_cache:
            cached_data, cached_time = self.price_cache[cache_key]
            if time.time() - cached_time < 300:
                return cached_data

        now = time.time()
        if now - self.last_api_call < 0.2:
            time.sleep(0.2 - (now - self.last_api_call))

        apis = [CONFIG['rest_api'], CONFIG['rest_api_backup']]
        headers = {'X-MBX-APIKEY': CONFIG['api_key']}

        for api in apis:
            try:
                url = f"{api}/api/v3/klines"
                params = {
                    'symbol': symbol,
                    'interval': '5m',
                    'limit': 1,
                    'endTime': close_time_ms
                }
                response = requests.get(url, params=params, headers=headers, timeout=5)
                response.raise_for_status()
                data = response.json()
                if data and len(data) > 0:
                    kline_data = data[0]
                    ohlc = {
                        'high': float(kline_data[2]),
                        'low': float(kline_data[3]),
                        'close': float(kline_data[4]),
                        'volume': float(kline_data[5])
                    }
                    self.price_cache[cache_key] = (ohlc, time.time())
                    if len(self.price_cache) > 100:
                        oldest_key = next(iter(self.price_cache))
                        del self.price_cache[oldest_key]
                    self.last_api_call = time.time()
                    return ohlc
            except Exception as e:
                print(Fore.RED + f"❌ API确认失败 ({api}): {e}")
                continue
        return None

    def update_with_confirmed_ohlc(self, high: float, low: float, close: float, volume: float):
        self.high_prices.append(float(high))
        self.low_prices.append(float(low))
        self.close_prices.append(float(close))
        self.volumes.append(float(volume))
        
        if len(self.close_prices) > self.max_history:
            self.close_prices = self.close_prices[-self.max_history:]
            self.high_prices = self.high_prices[-self.max_history:]
            self.low_prices = self.low_prices[-self.max_history:]
            self.volumes = self.volumes[-self.max_history:]
            
        self.last_update_time = now_utc()
        return self.calculate_technical_indicators()

    def calculate_technical_indicators(self, use_live_ohlc: dict = None):
        closes = self.close_prices.copy()
        highs = self.high_prices.copy()
        lows = self.low_prices.copy()
        
        if use_live_ohlc is not None:
            closes.append(float(use_live_ohlc['close']))
            highs.append(float(use_live_ohlc['high']))
            lows.append(float(use_live_ohlc['low']))

        if len(closes) < max(self.rsi_period, self.adx_period) + self.stoch_period + 50:
            return None

        try:
            closes_arr = np.array(closes, dtype=np.float64)
            highs_arr = np.array(highs, dtype=np.float64)
            lows_arr = np.array(lows, dtype=np.float64)

            rsi = talib.RSI(closes_arr, timeperiod=self.rsi_period)
            stoch_k, stoch_d = talib.STOCH(
                high=rsi, low=rsi, close=rsi,
                fastk_period=self.stoch_period,
                slowk_period=self.k_smooth, slowk_matype=0,
                slowd_period=self.d_smooth, slowd_matype=0
            )

            adx = talib.ADX(highs_arr, lows_arr, closes_arr, timeperiod=self.adx_period)
            plus_di = talib.PLUS_DI(highs_arr, lows_arr, closes_arr, timeperiod=self.adx_period)
            minus_di = talib.MINUS_DI(highs_arr, lows_arr, closes_arr, timeperiod=self.adx_period)

            if (np.isnan(stoch_k[-1]) or np.isnan(stoch_d[-1]) or 
                np.isnan(adx[-1]) or np.isnan(plus_di[-1]) or np.isnan(minus_di[-1])):
                return None

            indicators = TechnicalIndicators(
                timestamp=now_utc(),
                stoch_rsi_k=stoch_k[-1],
                stoch_rsi_d=stoch_d[-1],
                adx=adx[-1],
                plus_di=plus_di[-1],
                minus_di=minus_di[-1]
            )

            if use_live_ohlc is None:
                self.last_indicators = indicators
                
            return indicators

        except Exception as e:
            print(Fore.RED + f"❌ 技术指标计算失败: {e}")
            return None

# ================== 策略类 ==================
class Strategy:
    def __init__(self):
        self.predictions = {}
        self.actuals = {}
        self.wins = 0
        self.total_preds = 0
        self.pass_count = 0
        self.history = deque(maxlen=20)  # 历史记录

    def add_history_record(self, record):
        self.history.appendleft(record)

    def make_prediction(self, close_time_ms, direction):
        if direction != "FLAT":
            self.predictions[close_time_ms] = direction
            self.total_preds += 1

    def verify_prediction(self, close_time_ms, actual_direction):
        if close_time_ms in self.predictions:
            pred = self.predictions[close_time_ms]
            if pred == actual_direction:
                self.wins += 1
                result = "✅命中"
            else:
                result = "❌失败"
            del self.predictions[close_time_ms]
            return result
        return None

    def get_stats(self):
        win_rate = (self.wins / self.total_preds * 100) if self.total_preds > 0 else 0.0
        return {
            'wins': self.wins,
            'total': self.total_preds,
            'pass': self.pass_count,
            'win_rate': win_rate
        }

# ================== 显示管理器 ==================
class DisplayManager:
    def __init__(self):
        self.console_width = 80
        self._init_screen()

    def _init_screen(self):
        """打印固定标题和表格头"""
        print(Fore.CYAN + Style.BRIGHT + f"""
╔{'═'*(self.console_width-2)}╗
║{' '*(self.console_width-2)}║
║     🎯 BTC StochRSI + ADX + 订单流因子 - 5分钟周期 + 提前1分钟预测{' '*(self.console_width-60)}║
║     📊 多因子融合策略 | 固定布局 局部刷新{' '*(self.console_width-45)}║
║{' '*(self.console_width-2)}║
╚{'═'*(self.console_width-2)}╝{Style.RESET_ALL}
        """)
        # 预留统计面板行（第5行）、实时信息行（第6行）、空行（第7行）、表格起始行（第8行）
        for _ in range(5):
            print()
        self._draw_table_header()

    def _draw_table_header(self):
        """绘制表格头部"""
        print('╔══════╦════════╦════════╦════════╦════════╦════════╦══════════════╗')
        print('║   T  ║    P   ║ 实际方向 ║   K值  ║   D值  ║ 结果   ║   备注       ║')
        print('╠══════╬════════╬════════╬════════╬════════╬════════╬══════════════╣')

    def update_stats(self, stats):
        """更新统计面板（第5行）"""
        line = f"📊 总预测: {stats['total']}  |  命中: {stats['wins']}  |  胜率: {stats['win_rate']:.1f}%  |  PASS: {stats['pass']}"
        print('\033[s' + '\033[5;1H' + '\033[K' + line + '\033[u', end='', flush=True)

    def update_realtime(self, text):
        """更新实时信息行（第6行）"""
        print('\033[s' + '\033[6;1H' + '\033[K' + text + '\033[u', end='', flush=True)

    def draw_history(self, history):
        """绘制历史记录表格（从第8行开始）"""
        # 保存光标，移动到表格起始行，清空下方
        print('\033[s', end='')
        print('\033[8;1H', end='')
        print('\033[J', end='')  # 清除从第8行开始到屏幕底部

        # 打印表头
        self._draw_table_header()

        # 打印记录
        for rec in list(history)[:10]:
            time_str = rec['time'].strftime('%H:%M')
            pred = rec.get('pred', '--')
            actual = rec.get('actual', '--')
            k = f"{rec.get('k', 0):.2f}" if rec.get('k') is not None else ' -- '
            d = f"{rec.get('d', 0):.2f}" if rec.get('d') is not None else ' -- '
            result = rec.get('result', '--')
            remark = rec.get('remark', '')
            # 根据结果着色
            if '✅' in result:
                color = Fore.GREEN
            elif '❌' in result:
                color = Fore.RED
            elif '⏸️' in result:
                color = Fore.YELLOW
            else:
                color = Fore.WHITE
            line = (f"║ {time_str} ║ {pred:^6} ║ {actual:^6} ║ {k:>6} ║ {d:>6} "
                    f"║ {result:^6} ║ {remark:<12} ║")
            print(color + line + Style.RESET_ALL)

        # 打印表格结尾
        print('╚══════╩════════╩════════╩════════╩════════╩════════╩══════════════╝')
        print('\033[u', end='', flush=True)

# ================== 因子融合函数 ==================
def fuse_signals(stoch_dir, factors_dict, adx_value):
    """
    主次结合法融合信号
    返回 (final_dir, vote_counts, pass_reason)
    """
    triggers = []
    
    net_buy = factors_dict.get('net_buy_ratio', 0)
    if net_buy > CONFIG['net_buy_ratio_high']:
        triggers.append(('net_buy_high', 'UP', CONFIG['factor_weights']['net_buy_high']))
    elif net_buy > CONFIG['net_buy_ratio_medium']:
        triggers.append(('net_buy_medium', 'UP', CONFIG['factor_weights']['net_buy_medium']))
    elif net_buy < CONFIG['net_buy_ratio_low']:
        triggers.append(('net_buy_low', 'DOWN', CONFIG['factor_weights']['net_buy_low']))
    
    close_pos = factors_dict.get('close_position', 0.5)
    if close_pos > CONFIG['close_position_up']:
        triggers.append(('close_pos_up', 'UP', CONFIG['factor_weights']['close_pos_up']))
    elif close_pos < CONFIG['close_position_down']:
        triggers.append(('close_pos_down', 'DOWN', CONFIG['factor_weights']['close_pos_down']))
    
    avg_buy_ratio = factors_dict.get('avg_buy_vs_close', 1.0)
    if avg_buy_ratio > CONFIG['avg_buy_price_ratio_up']:
        triggers.append(('avg_buy_up', 'UP', CONFIG['factor_weights']['avg_buy_up']))
    elif avg_buy_ratio < CONFIG['avg_buy_price_ratio_down']:
        triggers.append(('avg_buy_down', 'DOWN', CONFIG['factor_weights']['avg_buy_down']))
    
    sell_ratio = factors_dict.get('sell_ratio', 0.5)
    if sell_ratio < CONFIG['sell_ratio_up']:
        triggers.append(('sell_ratio_up', 'UP', CONFIG['factor_weights']['sell_ratio_up']))
    elif sell_ratio > CONFIG['sell_ratio_down']:
        triggers.append(('sell_ratio_down', 'DOWN', CONFIG['factor_weights']['sell_ratio_down']))
    
    volume_shrink = factors_dict.get('volume_shrink', 1.0)
    if volume_shrink < CONFIG['volume_shrink_threshold']:
        triggers.append(('volume_shrink', 'DOWN', CONFIG['factor_weights']['volume_shrink']))
    
    amplitude_high = factors_dict.get('amplitude_high', False)
    if amplitude_high:
        trend_dir = 'UP' if close_pos > 0.5 else 'DOWN'
        triggers.append(('amplitude_trend', trend_dir, CONFIG['factor_weights']['amplitude_trend']))
    
    up_votes = sum(w for _, dir_, w in triggers if dir_ == 'UP')
    down_votes = sum(w for _, dir_, w in triggers if dir_ == 'DOWN')
    total_triggers = len(triggers)
    
    market_cond = 'TRENDING' if adx_value >= CONFIG['adx_trend_threshold'] else ('CHOPPY' if adx_value <= CONFIG['adx_choppy_threshold'] else 'NEUTRAL')
    
    if stoch_dir != "FLAT":
        if total_triggers < CONFIG['min_active_factors']:
            return "FLAT", {'up': up_votes, 'down': down_votes}, "因子不足"
        
        if stoch_dir == "UP":
            agree_votes = up_votes
        else:
            agree_votes = down_votes
        
        if market_cond == "TRENDING":
            required_ratio = 0.4 if CONFIG['adx_trend_loosen'] else CONFIG['consensus_ratio']
        elif market_cond == "CHOPPY":
            required_ratio = 0.7 if CONFIG['adx_choppy_tighten'] else CONFIG['consensus_ratio']
        else:
            required_ratio = CONFIG['consensus_ratio']

        total_votes = up_votes + down_votes
        if total_votes == 0:
            return "FLAT", {'up': up_votes, 'down': down_votes}, "无因子触发"
        
        agree_ratio = agree_votes / total_votes
        if agree_ratio >= required_ratio:
            return stoch_dir, {'up': up_votes, 'down': down_votes}, None
        else:
            return "FLAT", {'up': up_votes, 'down': down_votes}, f"一致比例不足({agree_ratio:.2f}<{required_ratio})"
    
    else:
        if total_triggers < CONFIG['min_active_factors']:
            return "FLAT", {'up': up_votes, 'down': down_votes}, "因子不足"
        
        all_up = (up_votes > 0 and down_votes == 0)
        all_down = (down_votes > 0 and up_votes == 0)
        if not (all_up or all_down):
            return "FLAT", {'up': up_votes, 'down': down_votes}, "因子方向不一致"
        
        total_strength = up_votes if all_up else down_votes
        if market_cond == "TRENDING":
            strength_threshold = CONFIG['backup_strength_threshold'] * 0.8
        elif market_cond == "CHOPPY":
            strength_threshold = CONFIG['backup_strength_threshold'] * 1.2
        else:
            strength_threshold = CONFIG['backup_strength_threshold']
        
        if total_strength >= strength_threshold:
            return ("UP" if all_up else "DOWN"), {'up': up_votes, 'down': down_votes}, None
        else:
            return "FLAT", {'up': up_votes, 'down': down_votes}, f"强度不足({total_strength:.2f}<{strength_threshold})"

# ================== 订单流WebSocket任务 ==================
async def trade_stream(order_flow: OrderFlowFactors):
    url = CONFIG['trade_url']
    while True:
        try:
            async with websockets.connect(url) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data.get('e') == 'trade':
                        await order_flow.process_trade(data)
        except Exception as e:
            print(Fore.RED + f"❌ 订单流WebSocket错误: {e}")
            await asyncio.sleep(5)

# ================== 主逻辑 ==================
async def main_loop():
    calculator = TechnicalCalculator()
    strategy = Strategy()
    display = DisplayManager()
    order_flow = OrderFlowFactors()

    # 启动订单流任务
    asyncio.create_task(trade_stream(order_flow))

    if not calculator.fetch_historical_klines():
        print(Fore.RED + "❌ 无法加载历史数据，程序退出")
        return

    # 同步历史数据到order_flow
    if calculator.close_prices:
        order_flow.close_prices = calculator.close_prices.copy()
        order_flow.high_prices = calculator.high_prices.copy()
        order_flow.low_prices = calculator.low_prices.copy()
        order_flow.volumes = calculator.volumes.copy()

    # 初始化显示统计
    display.update_stats(strategy.get_stats())

    current_kline = None
    last_close_time = None
    prediction_made = False
    last_prediction_kline_close_ms = None

    # 实时信息显示任务
    async def realtime_updater():
        while True:
            if order_flow.current_kline_start_ms:
                d = order_flow
                line = (f"📈 当前周期 O:{d.current_buy_volume:.3f}? 需要整理... 简化：使用净Delta")
                # 构建实时信息
                if d.current_total_volume > 0:
                    net = d.current_delta
                    buy = d.current_buy_volume
                    sell = d.current_sell_volume
                    ratio = (buy / sell) if sell > 0 else float('inf')
                    line = (f"📈 实时: 价格:{current_kline.close_price if current_kline else 0:.2f}  "
                            f"净Δ:{net:+.3f} 买入:{buy:.3f} 卖出:{sell:.3f} 买卖比:{ratio:.2f} 交易:{d.current_trade_count}")
                else:
                    line = f"📈 实时: 等待数据..."
                display.update_realtime(line)
            await asyncio.sleep(2)

    asyncio.create_task(realtime_updater())

    async with websockets.connect(CONFIG['kline_url']) as ws:
        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                if 'k' in data:
                    k = data['k']
                    open_time_ms = k['t']
                    close_time_ms = k['T']
                    open_price = float(k['o'])
                    high_price = float(k['h'])
                    low_price = float(k['l'])
                    close_price = float(k['c'])
                    volume = float(k['v'])
                    is_closed = k['x']

                    now_ms = datetime_to_timestamp(now_utc())
                    seconds_to_close = (close_time_ms - now_ms) / 1000

                    # 新K线开始
                    if current_kline is None or open_time_ms != datetime_to_timestamp(current_kline.open_time):
                        await order_flow.reset_for_new_kline(open_time_ms, close_time_ms)
                        current_kline = Kline5(
                            open_time=datetime.fromtimestamp(open_time_ms / 1000),
                            close_time=datetime.fromtimestamp(close_time_ms / 1000),
                            open_price=open_price,
                            high_price=high_price,
                            low_price=low_price,
                            close_price=close_price
                        )
                        prediction_made = False
                    else:
                        current_kline.high_price = max(current_kline.high_price, high_price)
                        current_kline.low_price = min(current_kline.low_price, low_price)
                        current_kline.close_price = close_price

                    # 提前预测
                    if seconds_to_close <= CONFIG['early_prediction_seconds'] and seconds_to_close > 0 and not prediction_made and close_time_ms != last_prediction_kline_close_ms:
                        last_prediction_kline_close_ms = close_time_ms
                        prediction_made = True

                        live_ohlc = {'high': high_price, 'low': low_price, 'close': close_price}
                        pred_indicators = calculator.calculate_technical_indicators(use_live_ohlc=live_ohlc)
                        if pred_indicators:
                            factors = await order_flow.get_current_factors(close_price, high_price, low_price)
                            
                            if pred_indicators.stoch_golden_cross:
                                stoch_dir = "UP"
                            elif pred_indicators.stoch_death_cross:
                                stoch_dir = "DOWN"
                            else:
                                stoch_dir = "FLAT"
                            
                            final_dir, votes, pass_reason = fuse_signals(stoch_dir, factors, pred_indicators.adx)
                            
                            # 构建历史记录
                            record = {
                                'time': current_kline.open_time,
                                'pred': final_dir if final_dir != "FLAT" else 'PASS',
                                'actual': '--',
                                'k': pred_indicators.stoch_rsi_k,
                                'd': pred_indicators.stoch_rsi_d,
                                'result': '⏸️PASS' if final_dir == "FLAT" else None,
                                'remark': pass_reason if pass_reason else ''
                            }
                            strategy.add_history_record(record)
                            if final_dir == "FLAT":
                                strategy.pass_count += 1
                            else:
                                strategy.make_prediction(close_time_ms, final_dir)
                            
                            # 更新统计和表格
                            display.update_stats(strategy.get_stats())
                            display.draw_history(strategy.history)

                    # K线收盘
                    if is_closed and close_time_ms != last_close_time:
                        confirmed = calculator.confirm_kline_ohlc('BTCUSDT', close_time_ms)
                        if confirmed:
                            high_price = confirmed['high']
                            low_price = confirmed['low']
                            close_price = confirmed['close']
                            volume = confirmed.get('volume', volume)

                        indicators = calculator.update_with_confirmed_ohlc(high_price, low_price, close_price, volume)
                        order_flow.update_ohlcv(close_price, high_price, low_price, volume)
                        
                        if indicators:
                            current_kline.indicators = indicators
                            current_kline.high_price = high_price
                            current_kline.low_price = low_price
                            current_kline.close_price = close_price
                            
                            # 验证预测
                            result = strategy.verify_prediction(close_time_ms, current_kline.direction)
                            if result:
                                # 更新历史记录
                                for rec in strategy.history:
                                    if rec.get('time') == current_kline.open_time and rec['result'] == '⏸️PASS':
                                        # PASS记录不需要验证
                                        pass
                                    elif rec.get('time') == current_kline.open_time and 'result' not in rec:
                                        rec['actual'] = current_kline.direction
                                        rec['result'] = result
                                        break
                            
                            # 更新统计和表格
                            display.update_stats(strategy.get_stats())
                            display.draw_history(strategy.history)
                        
                        last_close_time = close_time_ms
                        prediction_made = False
                        current_kline = None

            except Exception as e:
                print(Fore.RED + f"❌ WebSocket 错误: {e}")
                await asyncio.sleep(5)

def run():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_loop())

if __name__ == "__main__":
    run()