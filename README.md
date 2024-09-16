# Rithmic Trades Power Meter

* This is an application that determines the strength of the market by tracking the number of trades that hits the bid and lifts the offer.


## Prequisites

* You must have either a Rithmic Paper Trading or Rithmic Live account.
* You must pass Rithmic's conformance testing.
* After passing conformance testing, Rithmic will send you a four-character prefix. 
* In `md_stream.py`, find the following variable:
    * `rq.app_name`
    * Update its value by replacing `CHANGE_ME` with the prefix issued by Rithmic.
    
## Installation

Download the repo to your hard drive.

## Start App

After downloading the repo, `cd` to `python_rithmic_power_meter`.

Run the following command:


python python_rithmic_trades_power_meter.py [username] [password] [exchange] [contract symbol] [seconds_before_refresh]


For example, if your Rithmic credentials are **00000000-DEMO/password123** and you want to determine the strength of the **Crude Oil** market for the **December 2024**, then you would run the following command:

```
python rithmic_trades_power_meter.py 00000000-DEMO password123 NYMEX CLZ4 60
``` 

After starting the app, you will see a series messages showing how many trades have hit the bid and lifted the offer:

```
Trades Power Meter Cumulative:
Trades that have hit the bid:    1
Trades that have lifted the ask: 0

Trades Power Meter Last 60 Seconds:
Trades that have hit the bid:    1
Trades that have lifted the ask: 0
```

## Stop App

To stop the app, issue a KeyboardInterrupt, `Ctrl+C`.

## In Live Environment

If you wish to trade live, do the following:

* In `rithmic_trades_power_meter.py`, find the variable `system_name`.
* Change its value from `Rithmic Paper Trading` to `Rithmic 01`.
     


