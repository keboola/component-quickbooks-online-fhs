# README

This component is a custom Quickbooks implementation for FHS. It fetches accounting and business-related data from the Quickbooks API.

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

       ```
       Possible values: [
         "Class",
         "Department",
         "Total"
       ]
       ```

### Application Authorization

By authorizing the application, Keboola securely communicates with the QuickBooks API to manage authentication. The QuickBooks API uses an access token and a refresh token via OAuth, both of which expire:
- Access token: Valid for one hour
- Refresh token: Valid for 24 hours

After 24 hours, a new refresh token must be obtained from the API.

If a refresh token is lost (e.g., due to an incorrect company ID or a manually terminated component run), the following error message will appear: **"Failed to refresh access token, please re-authorize credentials: {"error":"invalid_grant"}"**. 

The only way to resolve this issues is to repeat the OAuth authorization process.

## How Token Management Works
1. The user completes the OAuth process.

2. During the first run of the component, the tokens obtained from OAuth are used for authentication.

    - If the component's first run occurs within 24 hours of the OAuth process, the refresh token remains the same.

    - If the component's first run occurs after 24 hours of the OAuth process, a new refresh token is obtained from the QuickBooks API.
    
    In both cases, both the refresh and access tokens are stored in the state file, along with a timestamp of the component's run time.

3. During subsequent component runs, the component checks if there is data present in the state file. If data is found, it compares the timestamp from OAuth with the timestamp from the state file. It will use the tokens with the most recent timestamp. Once the component finishes, the tokens are stored in the state file again, along with a new timestamp.

4. Additional measure that saves the new token into the state file during client initialization was added to prevent the component from losing the refresh token. This means that running the component is disabled in development branches.

By following these steps, the system ensures the proper handling of authorization for API requests.

### Company ID
To obtain the Company ID:

**QuickBooks Login -> Settings** (top right corner) -> **"Account and Settings" -> "Billing & Subscription"**

***Note:** Please ignore any spaces in the Company ID.*

### Date Parameters

If `start_date` and `end_date` are not specified, the component will request the API using the endpoint's default parameter, which is **Fiscal Year to Date**.

Required format: YYYY-MM-DD


## Available Endpoints

See the example below:

### Example Configuration
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

### Constraints
The Quickbooks extractor is unable to parse the listed reports below generically. The JSON responses for these reports will be output as **one** cell.

The primary keys for these tables are `start_date` and `end_date`, which enable users to run the component incrementally.

Reports that cannot be parsed:
1. CashFlow
2. GeneralLedger
3. ProfitAndLossDetail
4. TransactionList
5. TrialBalance

### Accounting Types

Based on different business models, some clients are required to report on different accounting types: Cash or Accrual.

For the following reports, the component performs two requests.
- One for cash accounting
- One for accrual accounting
 
Reports affected:
1. BalanceSheet
2. GeneralLedger
3. ProfitAndLoss
4. ProfitAndLossDetail


## Support
If the component is missing the endpoints or reports you are looking for, please submit a support ticket. 
