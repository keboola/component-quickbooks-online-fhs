# README

This component is a custom Quickbooks implementation for FHS. It fetches accounting and business related data from Quickbooks API.

## API documentation

[QuickBooks API documentation](https://developer.intuit.com/app/developer/qbo/docs/develop)

## Configuration

This component has two modes available:

1. **No input table** - If the component has no input table set, it accepts following parameters:
   - company_id (string) - ID of the company that the auth is assigned to.
   - endpoints (list) - list of endpoints the component should process.
   - destination.load_type (string) - either incremental_load or full_load

2. **Input table mapped** - If the component detects an input table, it will load settings from input table. However, the component still needs parameter company_id in order to run in input table mode:
   - Mandatory parameters for input table mode:
     - company_id (string) - ID of the company that the auth is assigned to.
   - Input table columns:
     ```
     "PK","report","start_date","end_date","segment_data_by"
     ```
     - PK is the company_id,
     - report is the name of report to fetch. Available reports are:
       ```
       "reports": [
         "ProfitAndLossQuery**",
         "BalanceSheet**",
         "CashFlow**",
         "GeneralLedger**",
         "ProfitAndLossDetail**",
         "TransactionList**",
         "TrialBalance**"
       ]
       ```
     - start_date and end_date are strings in the YYYY-MM-DD format.
     - segment_data_by (optional) If not empty, sends this parameter along with the request and is used for report grouping
