This component is a custom Quickbooks implementation for FHS. It fetches accounting and business related data from Quickbooks API.

## API Documentation

[QuickBooks API Documentation](https://developer.intuit.com/app/developer/qbo/docs/develop)

## Configuration

This component has two modes available:

1. **No input table** - If no input is set, the component accepts the following parameters:
   - `company_id` (string) - The ID of the company assigned to the authentication.
   - `endpoints` (list) - List of endpoints the component should process.
   - `destination.load_type` (string) - Either `incremental_load` or `full_load`.

2. **Input table mapped** - If an input table is detected, the component will load settings from the input table. However, the component still requires the `company_id` parameter to run in this mode:
   - Mandatory parameters for input table mode:
     - `company_id` (string) - The ID of the company assigned to the authentication.
   - Input table columns:
     ```
     "PK","report","start_date","end_date","segment_data_by"
     ```
     - `PK` is the **company_id**.
     - `report` is the name of the report to fetch. Available reports include:
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
     - `start_date` and `end_date` – Strings in YYYY-MM-DD format.
     - `segment_data_by` – A mandatory column used for report grouping. 
