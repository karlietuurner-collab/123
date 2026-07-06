import asyncio
import json
import time
import numpy as np
import websockets
from datetime import datetime, timedelta
from collections import deque
import logging
import os
import sys
import ssl
import requests

# 清屏
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'

class AdaptiveK:
    def __init__(self):
        self.k = 1.0
        self.alpha = 0.05
        self.history = deque(maxlen=100)
        self.volume_threshold = 1000000

    def update(self, F_active: float, F_static: float, volume: float, real_time_momentum: float):
        """更新K因子，使用实时动量替代加速度"""
        denom = np.sqrt(volume + 1e-6) * (abs(real_time_momentum) + 1e-6)
        if abs(denom) > 1e-6:
            k_theoretical = abs(F_active - F_static) / denom
            self.k = (1 - self.alpha) * self.k + self.alpha * k_theoretical
        
        if volume > self.volume_threshold:
            self.k *= 0.9
        self.k = max(0.01, min(100.0, self.k))
        self.history.append(self.k)
        return self.k

class BinanceDataCollector:
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        
        self.trade_buffer = deque(maxlen=2000)
        self.kline_buffer = deque(maxlen=50)
        self.orderbook_cache = None
        self.depth_updates = deque(maxlen=100)
        
        self.current_price = 0.0
        self.current_F_active = 0.0
        self.current_F_static = 0.0
        self.current_volume = 0.0
        self.current_real_time_momentum = 0.0
        self.current_momentum_components = {}
        
        self.ws_connected = {"trade": False, "kline": False, "depth": False}
        self.rest_api_connected = False
        
        self.reconnect_delays = {"trade": 1, "kline": 1, "depth": 1}
        self.max_reconnect_delay = 60
        
        self.api_endpoints = ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com"]
        self.ws_endpoints = ["wss://stream.binance.com:9443/ws/", "wss://stream.binance.com:443/ws/"]
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
    def get_adaptive_window(self) -> int:
        """自适应时间窗口"""
        if len(self.trade_buffer) < 50:
            return 15
            
        recent_prices = [trade['price'] for trade in list(self.trade_buffer)[-60:]]
        if len(recent_prices) < 10:
            return 15
            
        volatility = np.std(recent_prices) / np.mean(recent_prices) * 100
        
        current_time = time.time()
        recent_trades = [t for t in self.trade_buffer if current_time - t['trade_time'] <= 60]
        trade_density = len(recent_trades) / 60
        
        if volatility > 0.15 and trade_density > 3:
            return 8
        elif volatility > 0.08:
            return 12
        elif trade_density < 1:
            return 22
        else:
            return 15
        
    async def start_data_collection(self):
        tasks = [
            asyncio.create_task(self.collect_trade_stream_with_retry()),
            asyncio.create_task(self.collect_kline_stream_with_retry()),
            asyncio.create_task(self.collect_depth_stream_with_retry()),
            asyncio.create_task(self.update_orderbook_snapshot()),
            asyncio.create_task(self.calculate_realtime_metrics())
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # WebSocket连接方法保持不变
    async def collect_trade_stream_with_retry(self):
        while True:
            try:
                await self.collect_trade_stream()
            except Exception as e:
                self.logger.error(f"Trade stream error: {e}")
                self.ws_connected["trade"] = False
                delay = min(self.reconnect_delays["trade"], self.max_reconnect_delay)
                await asyncio.sleep(delay)
                self.reconnect_delays["trade"] = min(delay * 2, self.max_reconnect_delay)

    async def collect_kline_stream_with_retry(self):
        while True:
            try:
                await self.collect_kline_stream()
            except Exception as e:
                self.logger.error(f"Kline stream error: {e}")
                self.ws_connected["kline"] = False
                delay = min(self.reconnect_delays["kline"], self.max_reconnect_delay)
                await asyncio.sleep(delay)
                self.reconnect_delays["kline"] = min(delay * 2, self.max_reconnect_delay)

    async def collect_depth_stream_with_retry(self):
        while True:
            try:
                await self.collect_depth_stream()
            except Exception as e:
                self.logger.error(f"Depth stream error: {e}")
                self.ws_connected["depth"] = False
                delay = min(self.reconnect_delays["depth"], self.max_reconnect_delay)
                await asyncio.sleep(delay)
                self.reconnect_delays["depth"] = min(delay * 2, self.max_reconnect_delay)

    async def collect_trade_stream(self):
        for ws_base in self.ws_endpoints:
            uri = f"{ws_base}{self.symbol_lower}@trade"
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                async with websockets.connect(uri, ssl=ssl_context, ping_interval=30, ping_timeout=10, max_size=2**20) as ws:
                    self.ws_connected["trade"] = True
                    self.reconnect_delays["trade"] = 1
                    async for message in ws:
                        data = json.loads(message)
                        trade_info = {
                            'timestamp': data['T'],
                            'price': float(data['p']),
                            'quantity': float(data['q']),
                            'is_buyer_maker': data['m'],
                            'trade_time': data['T'] / 1000
                        }
                        self.trade_buffer.append(trade_info)
                        self.current_price = trade_info['price']
            except Exception:
                continue
        raise Exception("All trade stream endpoints failed")

    async def collect_kline_stream(self):
        for ws_base in self.ws_endpoints:
            uri = f"{ws_base}{self.symbol_lower}@kline_1m"
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                async with websockets.connect(uri, ssl=ssl_context, ping_interval=30, ping_timeout=10, max_size=2**20) as ws:
                    self.ws_connected["kline"] = True
                    self.reconnect_delays["kline"] = 1
                    async for message in ws:
                        data = json.loads(message)
                        kline = data['k']
                        if kline['x']:
                            kline_info = {
                                'timestamp': kline['t'],
                                'open': float(kline['o']),
                                'high': float(kline['h']),
                                'low': float(kline['l']),
                                'close': float(kline['c']),
                                'volume': float(kline['v']),
                                'close_time': kline['T']
                            }
                            self.kline_buffer.append(kline_info)
            except Exception:
                continue
        raise Exception("All kline stream endpoints failed")

    async def collect_depth_stream(self):
        for ws_base in self.ws_endpoints:
            uri = f"{ws_base}{self.symbol_lower}@depth@100ms"
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                async with websockets.connect(uri, ssl=ssl_context, ping_interval=30, ping_timeout=10, max_size=2**20) as ws:
                    self.ws_connected["depth"] = True
                    self.reconnect_delays["depth"] = 1
                    async for message in ws:
                        data = json.loads(message)
                        depth_info = {
                            'timestamp': time.time() * 1000,
                            'bids': data['b'][:10],
                            'asks': data['a'][:10]
                        }
                        self.depth_updates.append(depth_info)
            except Exception:
                continue
        raise Exception("All depth stream endpoints failed")

    async def update_orderbook_snapshot(self):
        while True:
            success = False
            for base_url in self.api_endpoints:
                try:
                    url = f"{base_url}/api/v3/depth"
                    params = {"symbol": self.symbol, "limit": 100}
                    resp = requests.get(url, params=params, timeout=10)
                    if resp.status_code == 200:
                        self.orderbook_cache = resp.json()
                        self.rest_api_connected = True
                        success = True
                        break
                except Exception:
                    continue
            if not success:
                self.rest_api_connected = False
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(20)

    async def calculate_realtime_metrics(self):
        while True:
            try:
                adaptive_window = self.get_adaptive_window()
                
                self.current_F_active = self.calculate_F_active(adaptive_window)
                self.current_F_static = self.calculate_F_static()
                self.current_volume = self.calculate_recent_volume(adaptive_window)
                
                momentum_data = self.calculate_real_time_momentum(adaptive_window)
                self.current_real_time_momentum = momentum_data['real_time_momentum']
                self.current_momentum_components = momentum_data
                
            except Exception as e:
                self.logger.error(f"Metrics calculation error: {e}")
            await asyncio.sleep(0.5)

    def calculate_real_time_momentum(self, window_seconds: int) -> dict:
        """实时动量指标"""
        money_flow_slope = self.calculate_money_flow_slope(window_seconds)
        depth_consumption_rate = self.calculate_depth_consumption_rate()
        order_flow_imbalance = self.calculate_order_flow_imbalance(window_seconds)
        
        real_time_momentum = (
            0.45 * money_flow_slope +
            0.35 * depth_consumption_rate +
            0.20 * order_flow_imbalance
        )
        
        return {
            'real_time_momentum': real_time_momentum,
            'money_flow_slope': money_flow_slope,
            'depth_consumption_rate': depth_consumption_rate,
            'order_flow_imbalance': order_flow_imbalance,
            'window_used': window_seconds
        }

    def calculate_money_flow_slope(self, window_seconds: int) -> float:
        """计算资金流斜率"""
        if len(self.trade_buffer) < 20:
            return 0.0
            
        current_time = time.time()
        money_flows = []
        time_points = []
        
        for trade in self.trade_buffer:
            if current_time - trade['trade_time'] <= window_seconds:
                direction = 1 if not trade['is_buyer_maker'] else -1
                money_flow = direction * (trade['price'] * trade['quantity'])
                money_flows.append(money_flow)
                time_points.append(trade['trade_time'])
        
        if len(money_flows) < 5:
            return 0.0
        
        cumulative_flow = np.cumsum(money_flows)
        
        if len(cumulative_flow) > 1:
            x = np.array(time_points)
            y = np.array(cumulative_flow)
            
            if len(x) > 1:
                velocity = np.diff(y) / (np.diff(x) + 1e-6)
                
                if len(velocity) > 1:
                    x_diff = np.diff(x[1:])
                    if len(x_diff) > 0:
                        acceleration = np.diff(velocity) / (x_diff + 1e-6)
                        return float(np.mean(acceleration[-3:])) if len(acceleration) > 0 else 0.0
        
        return 0.0

    def calculate_depth_consumption_rate(self) -> float:
        """计算深度消耗率"""
        if len(self.depth_updates) < 3:
            return 0.0
            
        try:
            recent_depths = list(self.depth_updates)[-3:]
            bid_depth_changes = []
            ask_depth_changes = []
            
            for i in range(1, len(recent_depths)):
                prev_depth = recent_depths[i-1]
                curr_depth = recent_depths[i]
                
                prev_bid_volume = sum([float(price) * float(qty) for price, qty in prev_depth['bids'][:5]])
                curr_bid_volume = sum([float(price) * float(qty) for price, qty in curr_depth['bids'][:5]])
                bid_change = (curr_bid_volume - prev_bid_volume) / (prev_bid_volume + 1e-6)
                bid_depth_changes.append(bid_change)
                
                prev_ask_volume = sum([float(price) * float(qty) for price, qty in prev_depth['asks'][:5]])
                curr_ask_volume = sum([float(price) * float(qty) for price, qty in curr_depth['asks'][:5]])
                ask_change = (curr_ask_volume - prev_ask_volume) / (prev_ask_volume + 1e-6)
                ask_depth_changes.append(ask_change)
            
            if bid_depth_changes and ask_depth_changes:
                bid_consumption = -np.mean(bid_depth_changes)
                ask_consumption = -np.mean(ask_depth_changes)
                return bid_consumption - ask_consumption
                
        except Exception as e:
            self.logger.error(f"Depth consumption calculation error: {e}")
            
        return 0.0

    def calculate_order_flow_imbalance(self, window_seconds: int) -> float:
        """计算订单流不平衡"""
        if len(self.trade_buffer) < 10:
            return 0.0
            
        current_time = time.time()
        buy_volume = 0.0
        sell_volume = 0.0
        
        for trade in self.trade_buffer:
            if current_time - trade['trade_time'] <= window_seconds:
                volume = trade['price'] * trade['quantity']
                if not trade['is_buyer_maker']:
                    buy_volume += volume
                else:
                    sell_volume += volume
        
        total_volume = buy_volume + sell_volume
        if total_volume == 0:
            return 0.0
            
        return (buy_volume - sell_volume) / total_volume

    def calculate_F_active(self, window_seconds: int) -> float:
        if not self.trade_buffer: return 0.0
        current_time = time.time()
        F_active = 0.0
        for trade in self.trade_buffer:
            if current_time - trade['trade_time'] <= window_seconds:
                direction = 1 if not trade['is_buyer_maker'] else -1
                F_active += direction * (trade['price'] * trade['quantity'])
        return F_active

    def calculate_F_static(self) -> float:
        if not self.orderbook_cache or not self.orderbook_cache.get('bids'):
            return 0.5
        try:
            bids = self.orderbook_cache['bids'][:20]
            asks = self.orderbook_cache['asks'][:20]
            bid_depth = sum(float(p) * float(q) for p, q in bids)
            ask_depth = sum(float(p) * float(q) for p, q in asks)
            if bid_depth + ask_depth == 0: return 0.5
            imbalance = (ask_depth - bid_depth) / (ask_depth + bid_depth)
            return imbalance * np.log1p(bid_depth + ask_depth)
        except Exception:
            return 0.5

    def calculate_recent_volume(self, window_seconds: int) -> float:
        if not self.trade_buffer: return 0.0
        current_time = time.time()
        volume = 0.0
        for trade in self.trade_buffer:
            if current_time - trade['trade_time'] <= window_seconds:
                volume += trade['price'] * trade['quantity']
        return volume

class WyckoffAnalyzer:
    def __init__(self):
        self.effort_threshold = 500000
        self.result_threshold = 0.001

    def detect_effort_result_divergence(self, F_active: float, price_change_30s: float, volume_30s: float) -> float:
        if abs(F_active) > self.effort_threshold and abs(price_change_30s / 100) < self.result_threshold:
            return -0.5 if F_active > 0 else 0.5
        return 0.0

class NewtonianPredictor:
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.data_collector = BinanceDataCollector(symbol)
        self.k_estimator = AdaptiveK()
        self.wyckoff_analyzer = WyckoffAnalyzer()
        
        self.prediction_history = deque(maxlen=5000)
        self.accuracy_stats = {"correct": 0, "total": 0}
        self.last_prediction = None
        self.pending_verifications = deque(maxlen=10)
        
        # 显示状态缓存
        self.display_cache = {}
        self.display_initialized = False
        
    def should_predict(self) -> bool:
        now = datetime.now()
        return now.minute % 5 == 4 and 30 <= now.second <= 50

    async def prediction_loop(self):
        last_window = None
        while True:
            try:
                now = datetime.now()
                window_key = f"{now.hour}:{(now.minute // 5)}"
                if self.should_predict() and window_key != last_window:
                    result = await self.predict_next_5min_direction()
                    if result:
                        self.data_collector.logger.info(f"新预测生成: {result['prediction']} 分数 {result['final_score']:.3f}")
                    last_window = window_key
            except Exception as e:
                self.data_collector.logger.error(f"Prediction loop error: {e}")
            await asyncio.sleep(1)

    async def predict_next_5min_direction(self):
        try:
            F_active = self.data_collector.current_F_active
            F_static = self.data_collector.current_F_static
            volume = self.data_collector.current_volume
            real_time_momentum = self.data_collector.current_real_time_momentum
            momentum_components = self.data_collector.current_momentum_components
            current_price = self.data_collector.current_price
            
            if volume <= 0: return None
                
            k_factor = self.k_estimator.update(F_active, F_static, volume, real_time_momentum)
            net_force = F_active - F_static
            mass = np.sqrt(volume + 1e-6)
            physics_momentum = net_force / (k_factor * mass + 1e-6)
            
            price_change_30s = self.calculate_price_change_30s()
            wyckoff_signal = self.wyckoff_analyzer.detect_effort_result_divergence(F_active, price_change_30s, volume)
            
            current_volatility = self.calculate_market_volatility()
            
            final_score = (
                0.30 * self.normalize_signal(F_active, 1000000) +
                0.15 * self.normalize_signal(physics_momentum, 100) +
                0.20 * self.normalize_signal(real_time_momentum, 1.0) +
                0.25 * (-self.normalize_signal(F_static, 1000)) +
                0.15 * wyckoff_signal
            )
            
            prediction = "UP" if final_score > 0 else "DOWN"
            confidence = min(abs(final_score), 1.0)
            
            now = datetime.now()
            current_5min_period = (now.minute // 5) * 5
            next_5min_period = current_5min_period + 5
            
            if next_5min_period >= 60:
                next_period_time = now.replace(hour=now.hour+1, minute=0, second=0, microsecond=0)
            else:
                next_period_time = now.replace(minute=next_5min_period, second=0, microsecond=0)
            
            verification_time = next_period_time + timedelta(seconds=5)
            
            prediction_result = {
                'id': len(self.prediction_history) + 1,
                'timestamp': now,
                'prediction': prediction,
                'confidence': confidence,
                'final_score': final_score,
                'current_price': current_price,
                'target_kline_period': next_period_time.strftime('%H:%M'),
                'verify_time': verification_time.strftime('%H:%M:%S'),
                'verify_time_timestamp': int(verification_time.timestamp() * 1000),
                'status': 'PENDING',
                'decision_factors': {
                    'F_active': F_active,
                    'F_static': F_static,
                    'real_time_momentum': real_time_momentum,
                    'money_flow_slope': momentum_components.get('money_flow_slope', 0),
                    'depth_consumption_rate': momentum_components.get('depth_consumption_rate', 0),
                    'order_flow_imbalance': momentum_components.get('order_flow_imbalance', 0),
                    'physics_momentum': physics_momentum,
                    'wyckoff_signal': wyckoff_signal,
                    'final_score': final_score,
                    'k_factor': k_factor,
                    'window_used': momentum_components.get('window_used', 15),
                    'market_volatility': current_volatility,
                    'volume': volume
                },
                'verification_result': None
            }
            
            self.prediction_history.append(prediction_result)
            self.last_prediction = prediction_result
            self.pending_verifications.append(prediction_result)
            return prediction_result
        except Exception as e:
            self.data_collector.logger.error(f"Prediction error: {e}")
            return None

    def calculate_market_volatility(self) -> float:
        if len(self.data_collector.trade_buffer) < 50:
            return 0.0
        
        recent_prices = [trade['price'] for trade in list(self.data_collector.trade_buffer)[-60:]]
        if len(recent_prices) < 10:
            return 0.0
        
        return np.std(recent_prices) / np.mean(recent_prices) * 100

    async def verify_predictions_with_rest_api(self):
        while True:
            current_time = datetime.now()
            
            for pred in list(self.pending_verifications):
                verify_time = datetime.fromtimestamp(pred['verify_time_timestamp'] / 1000)
                
                if current_time >= verify_time:
                    success = await self.verify_prediction_with_rest_api(pred)
                    if success:
                        self.accuracy_stats['total'] += 1
                        if pred['status'] == 'CORRECT':
                            self.accuracy_stats['correct'] += 1
                        
                        self.pending_verifications.remove(pred)
            
            await asyncio.sleep(15)

    async def verify_prediction_with_rest_api(self, prediction):
        try:
            target_time = datetime.fromtimestamp(prediction['verify_time_timestamp'] / 1000)
            kline_start_time = target_time - timedelta(minutes=5)
            
            for base_url in self.data_collector.api_endpoints:
                try:
                    url = f"{base_url}/api/v3/klines"
                    params = {
                        'symbol': self.symbol,
                        'interval': '5m',
                        'startTime': int(kline_start_time.timestamp() * 1000),
                        'endTime': int(target_time.timestamp() * 1000),
                        'limit': 1
                    }
                    
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        kline_data = response.json()
                        
                        if kline_data:
                            kline = kline_data[0]
                            open_price = float(kline[1])
                            close_price = float(kline[4])
                            
                            actual_direction = "UP" if close_price > open_price else "DOWN"
                            is_correct = prediction['prediction'] == actual_direction
                            
                            prediction['status'] = 'CORRECT' if is_correct else 'WRONG'
                            prediction['verification_result'] = {
                                'open_price': open_price,
                                'close_price': close_price,
                                'price_change_pct': ((close_price - open_price) / open_price) * 100,
                                'actual_direction': actual_direction,
                                'verification_source': 'REST_API_KLINE'
                            }
                            
                            return True
                
                except Exception as e:
                    self.data_collector.logger.error(f"REST API verification failed for {base_url}: {e}")
                    continue
            
            return False
            
        except Exception as e:
            self.data_collector.logger.error(f"Prediction verification error: {e}")
            return False

    def calculate_price_change_30s(self) -> float:
        if len(self.data_collector.trade_buffer) < 2:
            return 0.0
        current_time = time.time()
        trades = list(self.data_collector.trade_buffer)
        current_price = trades[-1]['price']
        for trade in reversed(trades):
            if current_time - trade['trade_time'] >= 30:
                return ((current_price - trade['price']) / trade['price']) * 100
        return 0.0

    def normalize_signal(self, value: float, scale: float) -> float:
        return np.tanh(value / scale)

    def move_cursor(self, row, col=1):
        """移动光标到指定位置"""
        print(f"\033[{row};{col}H", end="")

    def clear_line(self):
        """清除当前行"""
        print("\033[K", end="")

    def update_line(self, row, content):
        """更新指定行的内容"""
        self.move_cursor(row)
        self.clear_line()
        print(content, end="", flush=True)

    def initialize_display(self):
        """初始化显示界面（只执行一次）"""
        if self.display_initialized:
            return
            
        clear_screen()
        
        # 静态标题和框架（永不刷新）
        print("=" * 100)
        print(f"{Colors.BOLD}{Colors.CYAN}Newton's Force Prediction System - Enhanced v2.0{Colors.END}".center(100))
        print("=" * 100)
        print()
        
        # 预留行数用于动态内容
        print(" CONNECTION STATUS")
        print()  # 第6行：连接状态
        print()  # 第7行：连接数量
        print()
        print(" REAL-TIME DATA") 
        print()  # 第10行：当前价格
        print()  # 第11行：成交量
        print()  # 第12行：缓冲区大小
        print()
        print(" NEWTONIAN FORCES")
        print()  # 第15行：F_Active
        print()  # 第16行：F_Static
        print()  # 第17行：实时动量
        print()  # 第18行：K因子
        print()
        print(" MOMENTUM BREAKDOWN")
        print()  # 第21行：资金流斜率
        print()  # 第22行：深度消耗
        print()  # 第23行：订单流不平衡
        print()  # 第24行：自适应窗口
        print()
        print(" PREDICTION STATUS")
        print()  # 第27行：方向和置信度
        print()  # 第28行：目标K线
        print()  # 第29行：验证倒计时或结果
        print()
        print(" PERFORMANCE METRICS")
        print()  # 第32行：历史准确率
        print()  # 第33行：待验证
        print()  # 第34行：总预测数
        print()
        print(" RECENT PREDICTIONS")
        print("ID   Time      Dir   Conf   Score    Status     Result        Target")
        print("-" * 85)
        # 第38-45行：最近预测历史（100行）
        for i in range(8):
            print()
        print()
        print()  # 第47行：当前时间
        print("Auto-refresh: 2.0s | Press Ctrl+C to exit")
        
        self.display_initialized = True

    def display_terminal_dashboard(self):
        """更新终端面板（局部刷新）"""
        if not self.display_initialized:
            self.initialize_display()
            
        now = datetime.now()
        # 连接状态更新（第6行）
        trade_status = "●" if self.data_collector.ws_connected["trade"] else "○"
        kline_status = "●" if self.data_collector.ws_connected["kline"] else "○"
        depth_status = "●" if self.data_collector.ws_connected["depth"] else "○"
        rest_status = "●" if self.data_collector.rest_api_connected else "○"
        
        trade_color = Colors.GREEN if self.data_collector.ws_connected["trade"] else Colors.RED
        kline_color = Colors.GREEN if self.data_collector.ws_connected["kline"] else Colors.RED
        depth_color = Colors.GREEN if self.data_collector.ws_connected["depth"] else Colors.RED
        rest_color = Colors.GREEN if self.data_collector.rest_api_connected else Colors.RED
        
        conn_line = f"Trade: {trade_color}{trade_status}{Colors.END} | Kline: {kline_color}{kline_status}{Colors.END} | Depth: {depth_color}{depth_status}{Colors.END} | REST: {rest_color}{rest_status}{Colors.END}"
        
        # 只在状态真正变化时更新
        if self.display_cache.get('conn_line') != conn_line:
            self.update_line(6, conn_line)
            self.display_cache['conn_line'] = conn_line
        
        # 连接数量状态（第7行）
        active_connections = sum([
            self.data_collector.ws_connected["trade"],
            self.data_collector.ws_connected["kline"], 
            self.data_collector.ws_connected["depth"],
            self.data_collector.rest_api_connected
        ])
        
        if active_connections == 4:
            conn_color = Colors.GREEN
            conn_msg = "ALL SYSTEMS OPERATIONAL"
        elif active_connections >= 2:
            conn_color = Colors.YELLOW
            conn_msg = "PARTIAL CONNECTIVITY"
        else:
            conn_color = Colors.RED
            conn_msg = "CONNECTION ISSUES"
        
        conn_summary = f"Active: {conn_color}{active_connections}/4{Colors.END} - {conn_msg}"
        
        if self.display_cache.get('conn_summary') != conn_summary:
            self.update_line(7, conn_summary)
            self.display_cache['conn_summary'] = conn_summary

        # 实时数据更新（第10-12行）
        price_color = Colors.GREEN if self.data_collector.current_price > 71000 else Colors.RED
        price_line = f"Current Price: {price_color}${self.data_collector.current_price:,.2f}{Colors.END}"
        
        # 只在价格变化超过阈值时更新
        cached_price = self.display_cache.get('current_price', 0)
        if abs(self.data_collector.current_price - cached_price) > 0.5:
            self.update_line(10, price_line)
            self.display_cache['current_price'] = self.data_collector.current_price

        volume_line = f"30s Volume: ${self.data_collector.current_volume:,.0f}"
        if self.display_cache.get('volume_line') != volume_line:
            self.update_line(11, volume_line)
            self.display_cache['volume_line'] = volume_line

        buffer_line = f"Buffer: Trades({len(self.data_collector.trade_buffer)}) | Klines({len(self.data_collector.kline_buffer)}) | Depth({len(self.data_collector.depth_updates)})"
        if self.display_cache.get('buffer_line') != buffer_line:
            self.update_line(12, buffer_line)
            self.display_cache['buffer_line'] = buffer_line

        # 牛顿力学指标（第15-18行）
        f_active_color = Colors.GREEN if self.data_collector.current_F_active > 0 else Colors.RED
        f_active_line = f"F_Active (30s): {f_active_color}{self.data_collector.current_F_active:,.0f}{Colors.END} USDT"
        
        cached_f_active = self.display_cache.get('F_active', 0)
        if abs(self.data_collector.current_F_active - cached_f_active) > 100:
            self.update_line(15, f_active_line)
            self.display_cache['F_active'] = self.data_collector.current_F_active

        f_static_line = f"F_Static: {self.data_collector.current_F_static:+.3f}"
        if self.display_cache.get('f_static_line') != f_static_line:
            self.update_line(16, f_static_line)
            self.display_cache['f_static_line'] = f_static_line

        momentum_color = Colors.GREEN if self.data_collector.current_real_time_momentum > 0 else Colors.RED
        momentum_line = f"Real-Time Momentum: {momentum_color}{self.data_collector.current_real_time_momentum:+.6f}{Colors.END}"
        if self.display_cache.get('momentum_line') != momentum_line:
            self.update_line(17, momentum_line)
            self.display_cache['momentum_line'] = momentum_line

        k_factor_line = f"K Factor: {self.k_estimator.k:.4f}"
        if self.display_cache.get('k_factor_line') != k_factor_line:
            self.update_line(18, k_factor_line)
            self.display_cache['k_factor_line'] = k_factor_line

        # 动量分解（第21-24行）
        if self.data_collector.current_momentum_components:
            comp = self.data_collector.current_momentum_components
            
            money_flow_line = f"├─ Money Flow Slope: {comp.get('money_flow_slope', 0):+.6f}"
            if self.display_cache.get('money_flow_line') != money_flow_line:
                self.update_line(21, money_flow_line)
                self.display_cache['money_flow_line'] = money_flow_line

            depth_line = f"├─ Depth Consumption: {comp.get('depth_consumption_rate', 0):+.6f}"
            if self.display_cache.get('depth_line') != depth_line:
                self.update_line(22, depth_line)
                self.display_cache['depth_line'] = depth_line

            imbalance_line = f"├─ Order Flow Imbalance: {comp.get('order_flow_imbalance', 0):+.6f}"
            if self.display_cache.get('imbalance_line') != imbalance_line:
                self.update_line(23, imbalance_line)
                self.display_cache['imbalance_line'] = imbalance_line

            window_line = f"└─ Adaptive Window: {comp.get('window_used', 15)}s"
            if self.display_cache.get('window_line') != window_line:
                self.update_line(24, window_line)
                self.display_cache['window_line'] = window_line

        # 预测状态（第27-29行）
        if self.last_prediction:
            pred = self.last_prediction
            time_left = max(0, (datetime.fromtimestamp(pred['verify_time_timestamp'] / 1000) - now).total_seconds())
            
            direction = "▲ UP" if pred['prediction'] == "UP" else "▼ DOWN"
            dir_color = Colors.GREEN if pred['prediction'] == "UP" else Colors.RED
            conf = int(pred['confidence'] * 100)
            
            pred_line = f"Direction: {dir_color}{direction}{Colors.END} | Confidence: {Colors.YELLOW}{conf}%{Colors.END}"
            if self.display_cache.get('pred_line') != pred_line:
                self.update_line(27, pred_line)
                self.display_cache['pred_line'] = pred_line

            target_line = f"Target K-line: {pred['target_kline_period']} | Score: {pred['final_score']:+.4f}"
            if self.display_cache.get('target_line') != target_line:
                self.update_line(28, target_line)
                self.display_cache['target_line'] = target_line

            if time_left > 0:
                time_line = f"Verification in: {int(time_left//60)}m{int(time_left%60):02d}s"
            else:
                if pred['status'] == 'PENDING':
                    time_line = "Awaiting verification..."
                else:
                    status_color = Colors.GREEN if pred['status'] == 'CORRECT' else Colors.RED
                    time_line = f"Result: {status_color}{pred['status']}{Colors.END}"
                    
            if self.display_cache.get('time_line') != time_line:
                self.update_line(29, time_line)
                self.display_cache['time_line'] = time_line
        else:
            next_pred_time = 300 - ((now.minute % 5) * 60 + now.second - 4*60 - 30)
            if next_pred_time < 0:
                next_pred_time += 300
            next_line = f"Next Prediction: {int(next_pred_time//60)}m{int(next_pred_time%60):02d}s"
            if self.display_cache.get('next_line') != next_line:
                self.update_line(27, next_line)
                self.display_cache['next_line'] = next_line

        # 性能指标（第32-34行）
        accuracy = (self.accuracy_stats['correct'] / self.accuracy_stats['total'] * 100) if self.accuracy_stats['total'] > 0 else 0
        acc_color = Colors.GREEN if accuracy >= 60 else Colors.YELLOW if accuracy >= 50 else Colors.RED
        
        acc_line = f"Historical Accuracy: {acc_color}{accuracy:.1f}%{Colors.END} ({self.accuracy_stats['correct']}/{self.accuracy_stats['total']})"
        if self.display_cache.get('acc_line') != acc_line:
            self.update_line(32, acc_line)
            self.display_cache['acc_line'] = acc_line

        pending_line = f"Pending Verifications: {len(self.pending_verifications)}"
        if self.display_cache.get('pending_line') != pending_line:
            self.update_line(33, pending_line)
            self.display_cache['pending_line'] = pending_line

        total_line = f"Total Predictions: {len(self.prediction_history)}"
        if self.display_cache.get('total_line') != total_line:
            self.update_line(34, total_line)
            self.display_cache['total_line'] = total_line

        # 最近预测历史（第38-45行，仅在有新预测时更新）
        if self.prediction_history:
            current_history_len = len(self.prediction_history)
            if self.display_cache.get('history_len', 0) != current_history_len:
                for i, pred in enumerate(list(self.prediction_history)[-8:]):
                    status_color = Colors.GREEN if pred['status'] == 'CORRECT' else Colors.RED if pred['status'] == 'WRONG' else Colors.YELLOW
                    pred_color = Colors.GREEN if pred['prediction'] == 'UP' else Colors.RED
                    
                    if pred['verification_result']:
                        result_text = f"{pred['verification_result']['price_change_pct']:+.2f}%"
                    else:
                        result_text = "Pending"
                    
                    history_line = f"{pred['id']:<4} {pred['timestamp'].strftime('%H:%M:%S')}  {pred_color}{pred['prediction']:<5}{Colors.END} {pred['confidence']:.2f}  {pred['final_score']:+.3f}  {status_color}{pred['status']:<9}{Colors.END} {result_text:<12} {pred['target_kline_period']}"
                    
                    self.update_line(38 + i, history_line)
                
                self.display_cache['history_len'] = current_history_len

        # 当前时间（第47行）
        time_str = f"Current Time: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        if self.display_cache.get('time_str') != time_str:
            self.update_line(47, time_str)
            self.display_cache['time_str'] = time_str

    async def terminal_display_loop(self):
        """终端显示循环"""
        while True:
            try:
                self.display_terminal_dashboard()
                await asyncio.sleep(2.0)  # 降低刷新频率到2秒
            except Exception as e:
                self.data_collector.logger.error(f"Display error: {e}")
                await asyncio.sleep(5)

    async def start_prediction_system(self):
        print("Initializing Newton's Force Prediction System Enhanced v2.0...")
        print("Starting data collection streams...")
        print("Loading neural prediction engine...")
        print("Calibrating real-time momentum indicators...")
        await asyncio.sleep(2)
        
        await asyncio.gather(
            self.data_collector.start_data_collection(),
            self.prediction_loop(),
            self.terminal_display_loop(),
            self.verify_predictions_with_rest_api(),
            return_exceptions=True
        )

async def main():
    print(f"{Colors.BOLD}{Colors.CYAN}Starting Newton's Force Prediction System Enhanced v2.0...{Colors.END}")
    print(f"{Colors.YELLOW}Features:{Colors.END}")
    print(f"  • Real-time momentum analysis (money flow slope + depth consumption)")
    print(f"  • Adaptive time windows (8-22s based on volatility)")
    print(f"  • Precise K-line verification via REST API")
    print(f"  • Enhanced decision factor transparency")
    print(f"  • Anti-lag momentum indicators")
    print()
    
    predictor = NewtonianPredictor("BTCUSDT")

    try:
        await predictor.start_prediction_system()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}System shutdown requested by user{Colors.END}")
        print(f"{Colors.GREEN}Newton's Force Prediction System stopped gracefully{Colors.END}")
    except Exception as e:
        print(f"\n{Colors.RED}System error: {e}{Colors.END}")

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Colors.GREEN}Goodbye!{Colors.END}")