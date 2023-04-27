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

### Application Authorization

- By authorizing the application, KBC will safely communicate with QuickBooks API to handle the final authorization for API requests.

### Company ID

- To obtain Company ID:
  QuickBooks Login -> Settings(Top right corner) -> "Account and Setting" -> "Billing & Subscription"

  **Note: Please ignore the spaces in between the Company ID**

### Date Parameters

- If start_date and end_date are not specified, component will request the API with the endpoint's default parameter, Fiscal Year to Date
- Required format: YYYY-MM-DD


## Available Endpoints: ##
 Please see the example below:
### Example Config ###
```json
{
    "parameters": {
        "companyid": "9130356541726086",
        "endpoints": [
              "Account",
              "Bill",
              "BillPayment",
              "Budget",
              "Class",
              "Customer",
              "Department",
              "Deposit",
              "Invoice",
              "Item",
              "JournalEntry",
              "Payment",
              "Preferences",
              "Purchase",
              "PurchaseOrder",
              "TaxCode",
              "TaxRate",
              "Term",
              "Transfer",
              "Vendor"
        ],
        "destination": {
            "load_type": "full_load"
        }
    },
    "authorization": {"REDACTED":  "REDACTED"},
    "action": "run"
}
```

### Constraints ##
        - Quickbooks Extractor is unable to parse the listed reports below generically. The JSON returns of the requested reports will be output as *ONE* cell. The priamry key for these tables are start_date and end_date which enable users to run the component incrementally.
        - Records cannot parse:
            1. CashFlow
            2. GeneralLedger
            3. ProfitAndLossDetail
            4. TransactionList
            5. TrialBalance

### Accounting Types ##
        - Based on different business models, some clients are required to report on differnet accounting types: Cash or Accrual.
        - For reports below, component will perform 2 requests with 1 request against cash accounting type while the other against accrual accounting type
            1. BalanceSheet
            2. GeneralLedger
            3. ProfitAndLoss
            4. ProfitAndLossDetail


## Support ##
If the component is missing the endpoints or reports you are looking for, please submit a support ticket or feel free to contact me directly. 
         

## Contact Info ##
Leo Chan  
Vancouver, Canada (PST time)   
Email: leo@keboola.com  
Private: cleojanten@hotmail.com   