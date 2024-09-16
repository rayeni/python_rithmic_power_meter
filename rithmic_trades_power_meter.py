#!/usr/bin/env python3
'''
Created on September 15, 2024

Author: Rufus Ayeni

Contact: https://github.com/rayeni/python_rithmic_trading_app/discussions
'''

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import threading
import sys
from time import sleep
from md_stream import MdStreamApp

#################################################################
# VARIABLES                                                     #
#################################################################

# The url of the Rithmic server
uri              = 'wss://rprotocol.rithmic.com:443'

# The name of the Rithmic System. 
# For live trading, change to Rithmic 01.
system_name      = 'Rithmic Paper Trading'

# The username used to log into RTrader.
user_id          = sys.argv[1]

# The password used to log into RTrader. 
# Passed as a command line argument.
password         = sys.argv[2]

# The exchange used for trading: CBOT, NYMEX, or CME. 
# Passed as a command line argument.
exchange         = sys.argv[3]

# The contract to be traded. Examples: UBM4, CLH4.  
# Passed as a command line argument.
symbol           = sys.argv[4]

# Number of seconds before refresh
# Passed as a command line argument and converted to int.
window_seconds   = int(sys.argv[5])

# Create threading event variable.
shutdown_event = threading.Event()

#################################################################
# FUNCTIONS                                                     #
#################################################################

def md_thread_function():
    '''
    Start market data streaming in a separate daemon thread.
    '''

    # Since the market data streaming app uses asyncio,
    # a new event loop must be set for this thread.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Start the event loop and run the 
        # market data streaming application.
        md_stream_app.run()
    except asyncio.CancelledError:
        # Handle task cancellation, which might not be 
        # needed unless you cancel tasks explicitly.
        print("md_thread: Asyncio tasks were cancelled.")
    except Exception as e:
        # This will handle any other exceptions that might occur.
        print("md_thread: An error occurred:", e)
    finally:
        # Perform cleanup
        print("\nmd_thread: Cleaning up resources...")
        loop.close()

#################################################################
# START OF PROGRAM                                              #
#################################################################

#------RUN ONE-TIME TASKS------#

# Create market data streaming object
md_stream_app = MdStreamApp(uri, system_name, user_id, password, exchange, symbol, window_seconds)
# Create thread for market data streaming
md_stream_thread = threading.Thread(target=md_thread_function, daemon=True)
# Start the md_stream_thread
md_stream_thread.start()

sleep(5)

#################################################################
# START LOOP TO GET TRADES POWER METER UPDATES                  #
#################################################################
try:

    while True:
        
        # Lock the DataFrame for thread-safe operation 
        with md_stream_app.lock:
            # Get Power Meter Update
            if md_stream_app.trades_60_df is not None:
                print('Trades Power Meter Cumulative:')
                print(f'Trades that have hit the bid:    {md_stream_app.trades_df["sell_volume"].sum()}')
                print(f'Trades that have lifted the ask: {md_stream_app.trades_df["buy_volume"].sum()}')
            else:
                print('Trades Power Meter Cumulative: No trades data available.')
            print('')
            if md_stream_app.trades_60_df is not None:
                print('Trades Power Meter Last 60 Seconds:')
                print(f'Trades that have hit the bid:    {md_stream_app.trades_60_df["sell_volume"].sum()}')
                print(f'Trades that have lifted the ask: {md_stream_app.trades_60_df["buy_volume"].sum()}\n\n\n\n\n')
            else:
                print('Trades Power Meter Last 60 Seconds: No trades data available.\n\n\n\n\n')

        sleep(1)
            
except KeyboardInterrupt:
    print("\nKeyboardInterrupt received, shutting down md_stream_thread...")
    md_stream_app.cleanup()
    shutdown_event.set()
    md_stream_thread.join()
finally:
    print("Main thread exiting...")