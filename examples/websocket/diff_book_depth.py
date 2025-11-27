# websocket_client.py

import time
import logging
import sys
from aster.lib.utils import config_logging
from aster.websocket.client.stream import WebsocketClient as AsterWsClient
from aster.lib.errors import ConnectionError, SubscriptionError # Assuming specific error types

# --- Configuration ---
LOG_LEVEL = logging.DEBUG
SYMBOL = "btcusdt"
SPEED = 100 # Data update frequency in milliseconds
SUBSCRIPTION_ID = 1 
RUNTIME_SECONDS = 5 # Extended time to demonstrate data flow

# --- Logging Setup ---
try:
    config_logging(logging, LOG_LEVEL)
    logger = logging.getLogger(__name__)
except Exception as e:
    print(f"FATAL: Could not configure logging. Error: {e}", file=sys.stderr)
    sys.exit(1)


def message_handler(message: dict) -> None:
    """
    Handles incoming WebSocket messages (e.g., market data updates).

    Args:
        message: The parsed message payload from the WebSocket stream.
    """
    # Simply print the received message for demonstration purposes
    print(message)


def run_websocket_client() -> None:
    """Initializes and runs the Aster WebSocket client for market data subscription."""
    client = None
    try:
        # 1. Initialize the client
        client = AsterWsClient()
        logger.info("Aster WebSocket Client initialized.")

        # 2. Start the client (typically runs in a separate thread/process)
        client.start()
        logger.info("WebSocket connection established.")

        # 3. Subscribe to the Diff Book Depth stream
        client.diff_book_depth(
            symbol=SYMBOL,
            speed=SPEED,
            id=SUBSCRIPTION_ID,
            callback=message_handler,
        )
        logger.info("Subscribed to %s depth stream (ID: %s). Listening for data...", SYMBOL, SUBSCRIPTION_ID)

        # 4. Keep the main thread alive for a duration to receive data.
        # NOTE: Using time.sleep() blocks the main thread. In a real-world application, 
        # consider using an asyncio loop, event loop mechanisms, or a background worker 
        # to manage the main thread gracefully without blocking.
        time.sleep(RUNTIME_SECONDS)
        
    except (ConnectionError, SubscriptionError) as e:
        logger.error("A critical error occurred during WS operation: %s", e)
    except Exception as e:
        logger.critical("An unexpected error occurred: %s", e)
    finally:
        # 5. Stop the client and clean up resources gracefully.
        if client:
            logger.debug("Closing WS connection.")
            client.stop()
            logger.info("Client stopped successfully.")


if __name__ == "__main__":
    run_websocket_client()
