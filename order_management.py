import threading
import time
import queue
import json
import sqlite3
from datetime import datetime, time as dt_time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Union
import logging
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RequestType(Enum):
    UNKNOWN = 0
    NEW = 1
    MODIFY = 2
    CANCEL = 3

class ResponseType(Enum):
    UNKNOWN = 0
    ACCEPT = 1
    REJECT = 2

@dataclass
class Logon:
    username: str
    password: str

@dataclass
class Logout:
    username: str

@dataclass
class OrderRequest:
    symbol_id: int
    price: float
    qty: int
    side: str  # 'B' or 'S'
    order_id: int
    request_type: RequestType = RequestType.NEW
    timestamp: float = field(default_factory=time.time) # Automatically set current time if not provided
    
    def __post_init__(self):
        # Basic validation for order parameters
        if not isinstance(self.symbol_id, int) or self.symbol_id <= 0:
            raise ValueError("symbol_id must be a positive integer.")
        if not isinstance(self.price, (int, float)) or self.price < 0:
            raise ValueError("price must be a non-negative number.")
        
        # Relax qty validation for CANCEL requests
        if self.request_type == RequestType.CANCEL:
            if not isinstance(self.qty, int) or self.qty < 0: # Allow qty to be 0 for cancel
                raise ValueError("qty must be a non-negative integer for CANCEL requests.")
        else:
            if not isinstance(self.qty, int) or self.qty <= 0:
                raise ValueError("qty must be a positive integer for NEW/MODIFY requests.")
        
        if self.side not in ['B', 'S']:
            raise ValueError("side must be 'B' (Buy) or 'S' (Sell).")
        if not isinstance(self.order_id, int) or self.order_id <= 0:
            raise ValueError("order_id must be a positive integer.")
        if not isinstance(self.request_type, RequestType):
            raise ValueError("request_type must be a valid RequestType enum.")

@dataclass
class OrderResponse:
    order_id: int
    response_type: ResponseType
    timestamp: float = field(default_factory=time.time) # Automatically set current time if not provided

    def __post_init__(self):
        # Basic validation for order response parameters
        if not isinstance(self.order_id, int) or self.order_id <= 0:
            raise ValueError("order_id must be a positive integer for OrderResponse.")
        if not isinstance(self.response_type, ResponseType):
            raise ValueError("response_type must be a valid ResponseType enum.")

class PersistentStorage:
    """Handles persistent storage of order responses and latency data"""
    
    def __init__(self, db_path: str = "order_management.db"):
        self.db_path = db_path
        self._init_database()
        self._conn = None # For potential future connection pooling, currently not fully utilized for persistent connection

    def _get_connection(self):
        """Helper to get a database connection, with error handling."""
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def _init_database(self):
        """Initialize the database with required tables."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    response_type TEXT NOT NULL,
                    round_trip_latency_ms REAL NOT NULL,
                    sent_timestamp REAL NOT NULL,
                    received_timestamp REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def store_response(self, order_id: int, response_type: ResponseType, 
                      sent_timestamp: float, received_timestamp: float):
        """Store order response with latency information."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            latency_ms = (received_timestamp - sent_timestamp) * 1000
            
            cursor.execute('''
                INSERT INTO order_responses 
                (order_id, response_type, round_trip_latency_ms, sent_timestamp, received_timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (order_id, response_type.name, latency_ms, sent_timestamp, received_timestamp))
            
            conn.commit()
            logger.info(f"Stored response for order {order_id}: {response_type.name}, "
                       f"latency: {latency_ms:.2f}ms")
        except sqlite3.Error as e:
            logger.error(f"Error storing response for order {order_id}: {e}")
            raise
        finally:
            if conn:
                conn.close()

class TradingTimeManager:
    """Manages trading session times and logon/logout logic"""
    
    def __init__(self, start_time: dt_time, end_time: dt_time):
        self.start_time = start_time
        self.end_time = end_time
        self.is_logged_in = False
        self.last_check_date: Optional[datetime.date] = None # Tracks the date of the last session check

    def is_trading_time(self) -> bool:
        """Check if current time is within trading hours"""
        now = datetime.now()
        current_time = now.time()
        
        # Handle case where trading session crosses midnight
        if self.start_time <= self.end_time:
            in_trading_hours = self.start_time <= current_time <= self.end_time
        else: # e.g., 22:00 to 06:00
            in_trading_hours = current_time >= self.start_time or current_time <= self.end_time
        
        return in_trading_hours
    
    def should_logon(self) -> bool:
        """Check if system should send logon message"""
        now = datetime.now()
        current_date = now.date()
        
        # Only attempt to logon once per day when trading hours begin
        if self.is_trading_time() and not self.is_logged_in:
            if self.last_check_date != current_date:
                self.last_check_date = current_date
                return True
        return False
    
    def should_logout(self) -> bool:
        """Check if system should send logout message"""
        if not self.is_trading_time() and self.is_logged_in:
            self.last_check_date = None # Reset for next day's logon
            return True
        return False

class ThrottleManager:
    """Manages order throttling based on configured rate limits"""
    
    def __init__(self, orders_per_second: int):
        if not isinstance(orders_per_second, int) or orders_per_second <= 0:
            raise ValueError("orders_per_second must be a positive integer.")
        self.orders_per_second = orders_per_second
        self.sent_orders_count = 0
        self.current_second = int(time.time())
        self.lock = threading.Lock()
    
    def can_send_order(self) -> bool:
        """Check if an order can be sent immediately"""
        with self.lock:
            current_time_second = int(time.time())
            
            # Reset counter if we've moved to a new second
            if current_time_second != self.current_second:
                self.current_second = current_time_second
                self.sent_orders_count = 0
            
            return self.sent_orders_count < self.orders_per_second
    
    def record_sent_order(self):
        """Record that an order has been sent"""
        with self.lock:
            current_time_second = int(time.time())
            
            # Reset counter if we've moved to a new second
            if current_time_second != self.current_second:
                self.current_second = current_time_second
                self.sent_orders_count = 0
            
            self.sent_orders_count += 1

class OrderQueue:
    """Thread-safe order queue with modify/cancel handling"""
    
    def __init__(self):
        self.orders = deque()
        self.order_map: Dict[int, OrderRequest] = {}  # order_id -> order reference for quick lookup
        self.lock = threading.Lock()
    
    def add_order(self, order: OrderRequest):
        """Add a new order to the queue. Handles NEW, MODIFY, CANCEL as requests to be queued."""
        with self.lock:
            # For a NEW request, check if already in queue or sent (handled by OMS before calling add_order)
            # For MODIFY/CANCEL, it's a new request for an existing order or to be sent directly.
            self.orders.append(order)
            self.order_map[order.order_id] = order
            logger.info(f"Order {order.order_id} (Type: {order.request_type.name}) added to queue.")
    
    def modify_order(self, order_id: int, new_price: float, new_qty: int) -> bool:
        """Modify an existing order in the queue if it's there."""
        with self.lock:
            if order_id in self.order_map:
                existing_order = self.order_map[order_id]
                existing_order.price = new_price
                existing_order.qty = new_qty
                # Update timestamp to reflect modification time if it's still in queue
                existing_order.timestamp = time.time() 
                logger.info(f"Modified order {order_id} in queue: price={new_price}, qty={new_qty}")
                return True
            return False
    
    def cancel_order(self, order_id: int) -> bool:
        """Remove an order from the queue if it's there."""
        with self.lock:
            if order_id in self.order_map:
                order_to_remove = self.order_map[order_id]
                try:
                    self.orders.remove(order_to_remove) # deque.remove is O(N) but for small queues or infrequent cancels this is fine.
                                                        # For high performance, consider a different queue structure or marking as cancelled.
                except ValueError:
                    logger.warning(f"Order {order_id} not found in deque but in map during cancellation attempt.")
                    return False
                del self.order_map[order_id]
                logger.info(f"Cancelled order {order_id} from queue")
                return True
            return False
    
    def get_next_order(self) -> Optional[OrderRequest]:
        """Get the next order from the queue"""
        with self.lock:
            if self.orders:
                order = self.orders.popleft()
                # The order is being popped, so it's no longer 'in queue' for modification/cancellation
                if order.order_id in self.order_map: # Check to prevent KeyError if already removed by cancel
                    del self.order_map[order.order_id]
                return order
            return None
    
    def size(self) -> int:
        """Get current queue size"""
        with self.lock:
            return len(self.orders)

class OrderManagement:
    """Main order management system"""
    
    def __init__(self, start_time: dt_time, end_time: dt_time, 
                 orders_per_second: int = 100, username: str = "trader",
                 password: str = "password123"):
        
        # Core components
        self.time_manager = TradingTimeManager(start_time, end_time)
        self.throttle_manager = ThrottleManager(orders_per_second)
        self.order_queue = OrderQueue()
        self.storage = PersistentStorage()
        
        # Authentication
        self.username = username
        self.password = password
        
        # Order tracking: order_id -> timestamp when sent (for latency calculation)
        self.sent_orders: Dict[int, float] = {}
        self.sent_orders_lock = threading.Lock()
        
        # Control flags
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Worker threads
        self.session_manager_thread = None
        self.order_processor_thread = None
        
        logger.info(f"OrderManagement initialized with trading hours: "
                   f"{start_time} - {end_time}, throttle: {orders_per_second} orders/sec")
    
    def start(self):
        """Start the order management system"""
        if self.running:
            logger.warning("Order management system is already running.")
            return

        self.running = True
        self.shutdown_event.clear()  # Ensure event is cleared
        
        # Start session management thread
        self.session_manager_thread = threading.Thread(
            target=self._session_manager_worker, daemon=True)
        self.session_manager_thread.start()
        logger.info("Session manager thread started.")
        
        # Start order processing thread
        self.order_processor_thread = threading.Thread(
            target=self._order_processor_worker, daemon=True)
        self.order_processor_thread.start()
        logger.info("Order processor thread started.")
        
        logger.info("Order management system started successfully.")
    
    def stop(self):
        """Stop the order management system"""
        if not self.running:
            logger.warning("Order management system is not running.")
            return

        logger.info("Stopping order management system...")
        self.running = False
        self.shutdown_event.set() # Signal threads to shut down
        
        # Wait for threads to complete (with timeout)
        if self.session_manager_thread and self.session_manager_thread.is_alive():
            self.session_manager_thread.join(timeout=5)
            if self.session_manager_thread.is_alive():
                logger.warning("Session manager thread did not terminate gracefully.")
        
        if self.order_processor_thread and self.order_processor_thread.is_alive():
            self.order_processor_thread.join(timeout=5)
            if self.order_processor_thread.is_alive():
                logger.warning("Order processor thread did not terminate gracefully.")
        
        logger.info("Order management system stopped.")
    
    def onData(self, data: Union[OrderRequest, OrderResponse]):
        """
        Main entry point for incoming data (OrderRequest from upstream or OrderResponse from exchange).
        This method replaces the specific on_data_order_request and on_data_order_response
        to match the C++ style overloaded onData.
        """
        try:
            if isinstance(data, OrderRequest):
                self._on_order_request_internal(data)
            elif isinstance(data, OrderResponse):
                self._on_order_response_internal(data)
            else:
                logger.error(f"Received unknown data type: {type(data)}")
        except ValueError as ve:
            logger.error(f"Validation error for incoming data: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error in onData processing: {e}")

    def _on_order_request_internal(self, request: OrderRequest):
        """Internal handler for incoming order requests."""
        # Basic validation for the request object itself
        try:
            # The OrderRequest dataclass __post_init__ handles basic validation,
            # but ensure it's not None or malformed.
            if not isinstance(request, OrderRequest):
                raise ValueError("Incoming data is not a valid OrderRequest object.")
        except ValueError as ve:
            logger.warning(f"Invalid OrderRequest received: {ve}")
            return

        logger.info(f"Received order request {request.order_id} of type {request.request_type.name}")

        # Check if we're in trading hours
        if not self.time_manager.is_trading_time():
            logger.warning(f"Order {request.order_id} rejected - outside trading hours.")
            # In a real system, you might send a rejection response back upstream.
            return
        
        # Duplicate order ID detection for NEW orders
        if request.request_type == RequestType.NEW:
            with self.sent_orders_lock:
                if request.order_id in self.sent_orders or \
                   request.order_id in self.order_queue.order_map:
                    logger.warning(f"New order {request.order_id} rejected - duplicate order ID already tracked or in queue.")
                    # Potentially send a reject response upstream for duplicate new orders
                    return

        # Handle different request types
        if request.request_type == RequestType.NEW:
            self._handle_new_order(request)
        elif request.request_type == RequestType.MODIFY:
            self._handle_modify_request(request)
        elif request.request_type == RequestType.CANCEL:
            self._handle_cancel_request(request)
        else:
            logger.warning(f"Unknown request type {request.request_type} for order {request.order_id}")
    
    def _on_order_response_internal(self, response: OrderResponse):
        """Internal handler for incoming order responses from exchange."""
        # Basic validation for the response object itself
        try:
            if not isinstance(response, OrderResponse):
                raise ValueError("Incoming data is not a valid OrderResponse object.")
        except ValueError as ve:
            logger.warning(f"Invalid OrderResponse received: {ve}")
            return

        logger.info(f"Received order response for {response.order_id} of type {response.response_type.name}")
        try:
            with self.sent_orders_lock:
                if response.order_id in self.sent_orders:
                    sent_timestamp = self.sent_orders[response.order_id]
                    received_timestamp = response.timestamp
                    
                    # Store response with latency
                    self.storage.store_response(
                        response.order_id, response.response_type,
                        sent_timestamp, received_timestamp
                    )
                    
                    # Remove from tracking as response received
                    del self.sent_orders[response.order_id]
                    logger.debug(f"Removed order {response.order_id} from sent_orders tracking.")
                else:
                    logger.warning(f"Received response for untracked order {response.order_id}. "
                                   f"It might have been processed already or never sent by this OMS instance.")
        
        except Exception as e:
            logger.error(f"Error processing order response {response.order_id}: {str(e)}")
    
    def _handle_new_order(self, request: OrderRequest):
        """Handle new order request: attempt to send or queue if throttled."""
        if self.throttle_manager.can_send_order():
            self._send_order_to_exchange(request)
        else:
            self.order_queue.add_order(request)
            logger.info(f"New order {request.order_id} queued due to throttling.")
    
    def _handle_modify_request(self, request: OrderRequest):
        """Handle modify order request: attempt to modify in queue or send to exchange."""
        # Try to modify if the order is already in the queue
        if self.order_queue.modify_order(request.order_id, request.price, request.qty):
            logger.info(f"Modified queued order {request.order_id}.")
        else:
            # If not in queue, it might have been sent already or needs to be sent as a new modify request
            logger.info(f"Order {request.order_id} not found in queue for modification. Attempting to send as new modify request.")
            if self.throttle_manager.can_send_order():
                self._send_order_to_exchange(request)
            else:
                self.order_queue.add_order(request)
                logger.info(f"Modify order {request.order_id} queued due to throttling.")
    
    def _handle_cancel_request(self, request: OrderRequest):
        """Handle cancel order request: attempt to cancel from queue or send to exchange."""
        # Try to cancel from queue first
        if self.order_queue.cancel_order(request.order_id):
            logger.info(f"Cancelled queued order {request.order_id}.")
        else:
            # Order not in queue, it might have been sent already or needs to be sent as a new cancel request
            logger.info(f"Order {request.order_id} not found in queue for cancellation. Attempting to send cancel request to exchange.")
            if self.throttle_manager.can_send_order():
                self._send_order_to_exchange(request)
            else:
                self.order_queue.add_order(request)
                logger.info(f"Cancel order {request.order_id} queued due to throttling.")
    
    def _send_order_to_exchange(self, request: OrderRequest):
        """Send order to exchange and track it for response matching."""
        # Record timestamp BEFORE sending for accurate latency measurement
        # The timestamp in OrderRequest is already set on creation. We just ensure it's recorded here.
        current_time_before_send = time.time() 
        
        with self.sent_orders_lock:
            # Ensure we're not overwriting a timestamp for an already-sent order awaiting response.
            # This is particularly relevant for MODIFY/CANCEL requests on orders already sent.
            # For simplicity, we track the *latest* sent time if an ID is reused for modify/cancel.
            self.sent_orders[request.order_id] = current_time_before_send
        
        self.send(request) # This is the stub method that simulates sending to exchange
        self.throttle_manager.record_sent_order()
        
        logger.info(f"Sent order {request.order_id} (Type: {request.request_type.name}) to exchange.")
    
    def _session_manager_worker(self):
        """Worker thread for managing trading session (logon/logout)"""
        while self.running:
            try:
                # Use shutdown_event.wait() to introduce a delay and allow graceful shutdown
                if self.shutdown_event.wait(1): # Check every second
                    break # Exit if shutdown signaled

                if self.time_manager.should_logon():
                    self.send_logon()
                    self.time_manager.is_logged_in = True
                    logger.info("Sent logon message to exchange.")
                
                elif self.time_manager.should_logout():
                    self.send_logout()
                    self.time_manager.is_logged_in = False
                    logger.info("Sent logout message to exchange.")
            
            except Exception as e:
                logger.error(f"Error in session manager worker: {str(e)}")
    
    def _order_processor_worker(self):
        """Worker thread for processing queued orders"""
        while self.running:
            try:
                # Use shutdown_event.wait() for delay and graceful shutdown
                if self.shutdown_event.wait(0.01): # Check frequently for new orders, but allow shutdown
                    break # Exit if shutdown signaled
                
                if (self.time_manager.is_trading_time() and 
                    self.order_queue.size() > 0 and 
                    self.throttle_manager.can_send_order()):
                    
                    order = self.order_queue.get_next_order()
                    if order:
                        # Before sending, ensure this isn't a stale modify/cancel for an already processed order
                        # This scenario is partially handled by how modify/cancel work on the queue,
                        # but a final check here could add robustness. For now, we trust the queue management.
                        self._send_order_to_exchange(order)
            
            except Exception as e:
                logger.error(f"Error in order processor worker: {str(e)}")
    
    # Exchange interface methods (stubs as per requirements)
    def send(self, request: OrderRequest):
        """Send order request to exchange (stub implementation)"""
        # This method would send the request to the actual exchange
        # For now, it's just a placeholder as specified in requirements
        logger.debug(f"STUB: Sending order {request.order_id} to exchange.")
        pass
    
    def send_logon(self):
        """Send logon message to exchange (stub implementation)"""
        # This method would send logon to the actual exchange
        logger.debug("STUB: Sending logon message to exchange.")
        pass
    
    def send_logout(self):
        """Send logout message to exchange (stub implementation)"""
        # This method would send logout to the actual exchange
        logger.debug("STUB: Sending logout message to exchange.")
        pass

# Example usage and configuration
def create_sample_order_management():
    """Create a sample order management system for testing"""
    # Trading hours: Adjusted to a wider range for testing purposes.
    # Original: 10:00 AM to 1:00 PM IST
    # Adjusted to 9:00 AM to 5:00 PM IST
    start_time = dt_time(9, 0, 0)
    end_time = dt_time(17, 0, 0)
    
    # 100 orders per second throttle
    orders_per_second = 100
    
    return OrderManagement(start_time, end_time, orders_per_second)

if __name__ == "__main__":
    # Example usage
    oms = create_sample_order_management()
    oms.start()
    
    try:
        logger.info("Simulating trading operations...")
        time.sleep(2) # Give threads some time to start and for session manager to run

        # --- Simulate orders within trading hours ---
        
        # Test 1: New order within throttle limit
        logger.info("\n--- Simulating NEW order 1001 (should be sent immediately) ---")
        order_new_1 = OrderRequest(symbol_id=1, price=100.5, qty=1000, side='B', order_id=1001, request_type=RequestType.NEW)
        oms.onData(order_new_1)
        time.sleep(0.1)

        # Test 2: New order that gets queued (simulate exceeding throttle by setting a low throttle)
        logger.info("\n--- Simulating NEW order 1002 (should be queued if throttle is tight) ---")
        # Temporarily set a very low throttle for demonstration
        oms.throttle_manager.orders_per_second = 1
        order_new_2 = OrderRequest(symbol_id=2, price=200.75, qty=500, side='S', order_id=1002, request_type=RequestType.NEW)
        oms.onData(order_new_2)
        time.sleep(0.1) # Order 1002 should be queued
        
        # Test 3: Modify existing queued order (1002)
        logger.info("\n--- Simulating MODIFY order 1002 (should modify queued order) ---")
        order_modify_1002 = OrderRequest(symbol_id=2, price=201.0, qty=550, side='S', order_id=1002, request_type=RequestType.MODIFY)
        oms.onData(order_modify_1002)
        time.sleep(0.1)

        # Test 4: Cancel existing queued order (1002)
        logger.info("\n--- Simulating CANCEL order 1002 (should cancel queued order) ---")
        # For CANCEL, qty can be 0. The validation in OrderRequest has been adjusted.
        order_cancel_1002 = OrderRequest(symbol_id=2, price=0, qty=0, side='S', order_id=1002, request_type=RequestType.CANCEL) 
        oms.onData(order_cancel_1002)
        time.sleep(0.1)
        
        # Reset throttle for further tests
        oms.throttle_manager.orders_per_second = 100
        time.sleep(0.5) # Allow order processor to clear queue if any were sent/queued

        # Test 5: Simulate response for order 1001 (already sent)
        logger.info("\n--- Simulating response for order 1001 ---")
        response_1001 = OrderResponse(order_id=1001, response_type=ResponseType.ACCEPT)
        oms.onData(response_1001)
        time.sleep(0.1)

        # Test 6: New order during non-trading hours (will be rejected) - simulating by changing manager state
        logger.info("\n--- Simulating order 1003 (will be rejected if outside trading hours) ---")
        # Temporarily change the TradingTimeManager's state for simulation
        original_start = oms.time_manager.start_time
        original_end = oms.time_manager.end_time
        oms.time_manager.start_time = dt_time(0, 0, 0) # Make it look like outside trading hours
        oms.time_manager.end_time = dt_time(0, 0, 1)   # Very short window
        order_new_3 = OrderRequest(symbol_id=3, price=300.0, qty=700, side='B', order_id=1003, request_type=RequestType.NEW)
        oms.onData(order_new_3) # This should be rejected based on the mock time
        time.sleep(0.1)
        
        # Reset trading hours to original for cleanup
        oms.time_manager.start_time = original_start
        oms.time_manager.end_time = original_end

        # Keep running for a bit to see background workers
        logger.info("\n--- System running, monitoring logs for session and queued order processing ---")
        time.sleep(5) 
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down...")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}", exc_info=True)
    finally:
        oms.stop()
        logger.info("Application terminated.")