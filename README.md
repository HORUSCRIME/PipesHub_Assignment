**Order Management System (OMS)**
This project implements a lightweight Order Management System designed to process and manage trading orders, handle real-time market conditions like trading hours and order throttling, and ensure reliable communication with an exchange.

Table of Contents
Overview
Features
Design Decisions and Architecture
Setup Instructions
How to Run
Running Tests
Assumptions and Future Considerations
Requirements


**1. Overview**
The Order Management System (OMS) is built to efficiently manage the lifecycle of trading orders. It acts as a crucial intermediary between upstream systems (where orders originate) and an exchange, ensuring that orders are processed according to predefined rules and market conditions. The system focuses on robust handling of order submission, modification, and cancellation, all while adhering to constraints like trading hours and order submission rates.

**2. Features**
Trading Hour Enforcement: Only allows order submission during configured trading hours. Automatically sends Logon and Logout messages to the exchange at the start and end of the trading day.
Order Throttling: Limits the rate at which orders are sent to the exchange (e.g., X orders per second) to prevent overwhelming the exchange or hitting rate limits.
Concurrent Order Processing: Utilizes a multi-threaded architecture to handle incoming orders, session management, and queued order processing concurrently without blocking.
Intelligent Queue Management:
Orders that cannot be sent immediately (due to throttling or non-trading hours) are intelligently queued.
Supports MODIFY and CANCEL requests for orders still within the internal queue, updating or removing them before they reach the exchange.
Handles MODIFY and CANCEL requests for orders already sent to the exchange by re-submitting them as new requests (subject to throttling).
Persistent Storage: Stores OrderResponse details, including calculated round-trip latency, into an SQLite database for historical analysis and auditing.
Comprehensive Logging: Provides detailed logs for monitoring system behavior and debugging.

**
3. Design Decisions and Architecture**
Here’s a look at the Order Management System's design and how it's built:

Our Order Management System (OMS) is really built to handle all the incoming order requests, guide them to the exchange, and then process the responses. We've paid close attention to things like making sure orders only go out during specific trading hours, limiting how many orders we send at once (throttling), and managing modifications and cancellations smoothly within our queues. To make sure it's quick, can handle lots of things at once, and is easy to keep updated, we've gone with a modular, multi-threaded approach.

Breaking It Down: Our Modular, Component-Based Approach
We've designed the system by breaking it into distinct, focused pieces, each handling a specific part of the order management journey. This way of doing things helps us in a few key ways:

Keeping Things Tidy: Each part has one clear job, which makes the code much easier to understand, work on, and fix if something goes wrong.
Making It Reusable: Think of components like our TradingTimeManager, ThrottleManager, OrderQueue, and PersistentStorage as self-contained units. We can potentially pick them up and use them in other systems if needed!
Easy to Test: Because each piece is separate, we can test them individually. You can see this in our test_order_management.py file, which has dedicated tests for each manager and the queue.
Here’s a quick overview of the main parts:

OrderManagement (The Brains of the Operation): This is our central hub. It brings all the other components together, gets them started, and makes sure they talk to each other correctly. It's the main point of contact for incoming data (onData method) and handles the overall start-up and shutdown of the OMS.
TradingTimeManager: This component is all about keeping track of the clock. It knows if we're within the allowed trading hours and makes sure we send "Logon" messages to the exchange at the start of the trading day and "Logout" messages when it ends. A smart little detail here is using last_check_date to prevent us from trying to log in multiple times on the same day once we're in.
ThrottleManager: This is our traffic cop for orders. It makes sure we don't send too many orders per second. It counts how many orders have gone out and will hold back new ones if we hit our limit. It automatically resets its count every second, keeping things fair. We use a threading.Lock here to make sure everything stays organized even when multiple parts of the system are trying to send orders.
OrderQueue: This is our waiting room for orders that can't go out right away (maybe due to throttling or other reasons). It's designed to be super efficient and safe for multiple parts of the system to access at once. You can add, retrieve, modify, or even cancel orders that are still waiting in line. We use a dictionary (order_map) for quick lookups (like finding an order to modify by its ID) and a deque for fast adding and removing. Again, a threading.Lock keeps everything synchronized.
PersistentStorage: This part is dedicated to saving our OrderResponse data – things like the type of response, the order's ID, and how long the order took to complete (round-trip latency) – into an SQLite database. It keeps our core OMS logic from worrying about the nitty-gritty of database interactions and even handles database errors.
Data Structures (Like Logon, Logout, OrderRequest): These are straightforward blueprints for the messages that flow through our system and to the exchange. Using Python's dataclass makes defining these really clean and simple. They even include basic checks when they're created (__post_init__ methods) to make sure the data is valid. We also use Enum classes for things like RequestType and ResponseType to keep our code easy to read and prevent silly errors.
Working Together: Our Concurrency and Threading Model
We rely on Python's threading module to make sure our system is always responsive. This is vital for an OMS that's constantly receiving new data, doing background work, and dealing with time-sensitive tasks.

_session_manager_worker (Dedicated Thread): This thread has one job: to regularly check the trading hours and send "Logon" or "Logout" messages to the exchange when needed. It works in the background, making sure our session with the exchange is always managed without slowing down order processing.
_order_processor_worker (Dedicated Thread): This thread is constantly watching our OrderQueue. When the ThrottleManager gives the green light and we're within trading hours, it tries to send the queued orders to the exchange. This asynchronous approach means that when new orders come in (onData method), they don't get stuck waiting for slow network operations.
Main Thread (onData): The onData method, where we receive orders from upstream systems, is designed to be super quick. It does a fast check on incoming orders (validation, initial throttle check) and either queues them or sends them out if possible. Any heavy lifting or potentially slow operations (like actually talking to the exchange) are handed off to our worker threads. This way, our system can take in a flood of orders without freezing up.
Keeping It Safe (Thread Safety): We use threading.Lock objects quite a bit, especially within our ThrottleManager, OrderQueue, and when tracking sent_orders. This is like a traffic light, making sure that when multiple threads try to access the same data, they don't step on each other's toes and mess things up.
How We Tackle Specific Challenges
Trading Hours: Our TradingTimeManager is the central point for all trading session logic. If an order comes in outside these pre-set times, it's immediately rejected. "Logon" and "Logout" messages are sent precisely at the beginning and end of the trading window.
Throttling: The ThrottleManager uses a "sliding window" approach (per second) to limit how many orders we send. If we hit our limit, new orders are automatically put in a queue to be processed in the next second.
Managing Queued Orders (Modifies/Cancels): Our OrderQueue is smart about handling modifications and cancellations for orders that are still waiting in the queue.
Modify: If we get a request to Modify an order that's already in the queue, we just update its price and quantity right there in the queued order. This is great because it means we don't send an unnecessary modify message to the exchange if the order hasn't even left our system yet.
Cancel: If someone wants to Cancel an order that's still in the queue, we simply remove it. This avoids sending an order only to immediately cancel it.
If a modify or cancel request comes in for an order that's not in our queue, we assume it's already gone to the exchange. In that case, the modify/cancel request itself is then treated as a new request and is subject to our throttling rules.
Matching Responses and Calculating Speed: The OrderManagement class keeps a record of when each order was sent (sent_orders dictionary). When an OrderResponse comes back from the exchange, we match it up using the order_id, figure out how long the whole process took (round-trip latency), and then save all this information, along with the response_type, to our database.
Catching Errors and Keeping a Log
Staying Strong: We've put try-except blocks in various places (especially where data comes in and in our worker threads) to handle unexpected problems gracefully. This prevents the whole system from crashing if something goes wrong.
What's Happening?: We use Python's logging module to keep a detailed record of what the system is doing, at different levels of detail (INFO, WARNING, ERROR, DEBUG). This is super helpful for keeping an eye on things, figuring out why something might not be working, and generally understanding the flow of orders.


**4. Setup Instructions**
Clone the Repository:

Bash

git clone <repository_url>
cd <repository_name>
(Replace <repository_url> and <repository_name> with your actual repository details)

Ensure Python is Installed:
This project is built with Python. Make sure you have Python 3.8+ installed on your system.

Bash

python3 --version
No External Libraries:
A key design constraint for this project was to avoid third-party libraries. All required modules (threading, time, sqlite3, datetime, enum, dataclasses, collections, logging, unittest.mock) are part of Python's standard library. So, no pip install commands are needed!
**
5. How to Run**
The order_management.py script contains a main function with an example simulation.

To run the example simulation:

Bash

python3 order_management.py
This will start the OMS, simulate incoming orders (NEW, MODIFY, CANCEL), and demonstrate its core functionalities including trading hours, throttling, and queue management. You will see log messages in your console.

**6. Running Tests
**The project includes a comprehensive suite of unit, integration, performance, and stress tests in test_order_management.py.

To run all tests:

Bash

python3 test_order_management.py
The output will show a detailed report of all tests run, including any failures or errors.

**7. Assumptions and Future Considerations
**This section details explicit and implicit assumptions made during development and potential areas for future enhancement.

TCP/Shared Memory (Upstream Input): We assumed that the onData method would receive orders from various upstream systems via TCP or shared memory – the actual implementation of that part wasn't our focus.
Exchange Communication (send, sendLogon, sendLogout): The methods that actually send data to the exchange are just placeholders for now. We didn't need to implement the detailed communication protocols, allowing us to concentrate on the OMS's internal logic.
Exchange Communication Safety: The problem statement noted that the exchange's send methods aren't "thread safe." Our current setup helps with this because only one of our worker threads (_order_processor_worker) actually calls the send method. So, from our OMS's perspective, these calls are happening one at a time. If a real exchange required multiple threads to send data simultaneously, we'd need another layer to make sure calls are handled sequentially.
Unique Order IDs: We're assuming that all new order requests will have unique order IDs. If we get a duplicate, we reject it to avoid any confusion. This is pretty standard practice.
Storing Data (SQLite): We chose SQLite for saving data because it's simple and built right into the system, which works well for a self-contained setup. For a larger, high-traffic system, we'd definitely look at more robust database options like PostgreSQL.
Time Is Everything: We implicitly assumed that the system's clock is accurate and synchronized. This is crucial for calculating latencies correctly and for all our time-based operations like trading hours and throttling.
Advanced Error Handling for Stored Responses: While we handle basic errors when saving responses, things like automatic retries for failed database writes aren't explicitly in place, but they'd be a consideration for a production-ready system.

**8. Requirements
**Python 3.8+
Standard Python libraries only (no external dependencies required).
