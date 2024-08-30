import json
import logging
import requests
import dateparser
import urllib.parse as url_parse
from requests.auth import HTTPBasicAuth
from typing import Tuple
from keboola.component.base import ComponentBase  # noqa
import backoff
from requests.exceptions import HTTPError
from mapping import Mapping
from ratelimit import limits, sleep_and_retry

requesting = requests.Session()


class QuickBooksClientException(Exception):
    pass


class QuickbooksClient:
    """
    QuickBooks Requests Handler
    """

    def __init__(self, company_id, access_token, refresh_token, oauth, sandbox):
        self.count = None
        self.end_date = None
        self.start_date = None
        self.maxresults = None
        self.startposition = None
        self.report_api_bool = None
        self.endpoint = None
        self.data_2 = None
        self.data = None
        self.app_key = oauth.appKey
        self.app_secret = oauth.appSecret

        if not sandbox:
            self.base_url = "https://quickbooks.api.intuit.com/v3/company"
        else:
            self.base_url = "https://sandbox-quickbooks.api.intuit.com/v3/company"

        # Parameters for request
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.access_token_refreshed = False
        self.company_id = company_id
        self.reports_required_accounting_type = [
            "ProfitAndLoss",
            "ProfitAndLossDetail",
            "GeneralLedger",
            "BalanceSheet",
            "TrialBalance"
        ]

    def get_new_refresh_token(self) -> Tuple[str, str]:
        try:
            self.refresh_access_token()
        except Exception as e:
            raise QuickBooksClientException(e) from e

        return self.refresh_token, self.access_token

    def fetch(self, endpoint, report_api_bool, start_date, end_date, query="", params=None):
        """
        Fetching results for the specified endpoint
        """
        # Initializing Parameters
        self.endpoint = endpoint
        self.report_api_bool = report_api_bool

        # Pagination Parameters
        self.startposition = 1
        self.maxresults = 1000
        self.start_date = start_date
        self.end_date = end_date

        # data = Accrual Type
        # data2 = Cash Type
        self.data = []  # stores all the returns from request
        self.data_2 = []

        if report_api_bool:
            if self.endpoint == "CustomQuery":
                if query == '':
                    raise QuickBooksClientException("Please enter query for CustomQuery. Exit...")
                logging.debug("Input Custom Query: {0}".format(self.start_date))
                self.custom_request(input_query=query)
            else:
                if not (self.start_date and self.end_date):
                    raise QuickBooksClientException(f"Start date and End date are required for {endpoint} reports.")
                self.report_request(endpoint, start_date, end_date, params)
        else:
            self.count = self.get_count()  # total count of records for pagination
            if self.count == 0:
                logging.info(
                    "There are no returns for {0}".format(self.endpoint))
                self.data = []
            else:
                self.data_request()

    @backoff.on_exception(backoff.expo, HTTPError, max_tries=3)
    def refresh_access_token(self):
        """
        Get a new access token with refresh token.
        Also saves the new token in statefile.
        """
        logging.info("Refreshing Access Token")

        url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        param = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }

        r = requests.post(url, auth=HTTPBasicAuth(self.app_key, self.app_secret), data=param)
        r.raise_for_status()

        results = r.json()

        if "error" in results:
            raise QuickBooksClientException(f"Failed to refresh access token, please re-authorize credentials:"
                                            f" {r.text}")

        self.access_token = results["access_token"]
        self.refresh_token = results["refresh_token"]
        self.access_token_refreshed = True

    def get_count(self):
        """
        Fetch the number of records for the specified endpoint
        """

        # Request Parameters
        endpoint = self.endpoint
        url = "select count(*) from {0}".format(endpoint)
        encoded_url = self.url_encode(url)
        count_url = "{0}/{1}/query?query={2}".format(
            self.base_url, self.company_id, encoded_url)

        # Request the number of counts
        data = self._request(count_url)

        total_counts = data["QueryResponse"]["totalCount"]
        logging.debug("Total Number of Records for {0}: {1}".format(
            endpoint, total_counts))

        return total_counts

    @staticmethod
    def url_encode(query):
        """
        URL encoded the query parameter
        """
        out = url_parse.quote_plus(query)
        return out

    # API rate limits: https://developer.intuit.com/app/developer/qbo/docs/learn/rest-api-features#limits-and-throttles
    @sleep_and_retry
    @limits(calls=400, period=60)
    def _request(self, url, params=None):
        """
        Handles Request
        """
        # add minorversion to params
        if not params:
            params = {}
        params["minorversion"] = 70

        results = None
        request_success = False
        while not request_success:
            headers = {
                "Authorization": "Bearer " + self.access_token,
                "Accept": "application/json"
            }
            logging.debug(f'Requesting: {url} with params: {params}')
            data = requesting.get(url, headers=headers, params=params)

            try:
                results = data.json()
                logging.debug(f"Response: {results}")

            except json.decoder.JSONDecodeError as e:
                raise QuickBooksClientException(f"Cannot decode response: {data.text}") from e

            if "fault" in results or "Fault" in results:
                if not self.access_token_refreshed:
                    self.refresh_access_token()
                else:
                    if data:
                        error = data.json().get("fault").get("error")[0]
                        if error:
                            if error.get("message"):
                                raise QuickBooksClientException(f"Authorization failed. Please check Company ID and/or "
                                                                f"reauthorize the application: {error.get('message')}")
                            else:
                                raise QuickBooksClientException(error)
                        raise QuickBooksClientException(data.text)
                    else:
                        raise QuickBooksClientException(f"Client cannot fetch data from url {url}, please check "
                                                        f"defined endpoints and company_id.")
            else:
                request_success = True

        if not results:
            raise QuickBooksClientException("Unable to fetch results.")
        return results

    def data_request(self):
        """
        Handles Request Parameters and Pagination
        """

        num_of_run = 0

        while self.startposition <= self.count:
            # Query Parameters
            # Custom query for Class endpoint
            if self.endpoint == 'Class':

                query = "SELECT * FROM {0} WHERE Active IN (true, false) STARTPOSITION {1} MAXRESULTS {2}".format(
                    self.endpoint, self.startposition, self.maxresults)

            else:

                query = "SELECT * FROM {0} STARTPOSITION {1} MAXRESULTS {2}".format(
                    self.endpoint, self.startposition, self.maxresults)

            logging.debug("Request Query: {0}".format(query))
            encoded_query = self.url_encode(query)
            url = "{0}/{1}/query?query={2}".format(
                self.base_url, self.company_id, encoded_query)

            # Requests and concatenating results into class's data variable
            results = self._request(url)

            # If API returns error, raise exception and terminate application
            if "fault" in results or "Fault" in results:
                raise QuickBooksClientException(results)

            data = results["QueryResponse"][self.endpoint]

            # Concatenate with exist extracted data
            self.data = self.data + data

            if len(self.data) > 5_000:
                logging.info(f"Writing {len(self.data)} rows from {self.endpoint} endpoint to output file.")
                Mapping(endpoint=self.endpoint, data=self.data)

                self.data = []

            # Handling pagination parameters
            self.startposition += self.maxresults
            num_of_run += 1

        logging.debug("Number of Requests: {0}".format(num_of_run))

    def custom_request(self, input_query):
        """
        Handles Request Parameters and Pagination
        """

        # Query Parameters
        query = "{0}".format(input_query)

        logging.debug("Request Query: {0}".format(query))
        encoded_query = self.url_encode(query)
        url = "{0}/{1}/query?query={2}".format(
            self.base_url, self.company_id, encoded_query)

        # Requests and concatenating results into class's data variable
        results = self._request(url)

        # If API returns error, raise exception and terminate application
        if "fault" in results or "Fault" in results:
            raise Exception(results)

        data = results["QueryResponse"]

        # Concatenate with exist extracted data
        self.data = data

    def report_request(self, endpoint, start_date, end_date, params=None):
        """
        API request for Report Endpoint
        """

        if start_date == "":
            date_param = ""

            # For GeneralLedger ONLY
            if endpoint == "GeneralLedger":
                date_param = "?columns=klass_name,account_name,account_num,chk_print_state,create_by,create_date," \
                             "cust_name,doc_num,emp_name,inv_date,is_adj,is_ap_paid,is_ar_paid,is_cleared,item_name," \
                             "last_mod_by,last_mod_date,memo,name,quantity,rate,split_acc,tx_date,txn_type,vend_name," \
                             "net_amount,tax_amount,tax_code,dept_name,subt_nat_amount,rbal_nat_amount,debt_amt," \
                             "credit_amt "
        else:

            startdate = (dateparser.parse(start_date)).strftime("%Y-%m-%d")
            enddate = (dateparser.parse(end_date)).strftime("%Y-%m-%d")

            if startdate > enddate:
                raise Exception(
                    "Please validate your date parameter for {0}".format(endpoint))

            date_param = "?start_date={0}&end_date={1}".format(
                startdate, enddate)

            # For GeneralLedger ONLY
            if endpoint == "GeneralLedger":
                date_param = date_param + "&columns=dklass_name,account_name,account_num,chk_print_state," \
                                          "create_by,create_date,cust_name,doc_num,emp_name,inv_date,is_adj," \
                                          "is_ap_paid,is_ar_paid," \
                                          "is_cleared,item_name,last_mod_by,last_mod_date,memo,name,quantity,rate," \
                                          "split_acc,tx_date," \
                                          "txn_type,vend_name,net_amount,tax_amount,tax_code,dept_name," \
                                          "subt_nat_amount,rbal_nat_amount,debt_amt,credit_amt"

        url = "{0}/{1}/reports/{2}{3}".format(self.base_url,
                                              self.company_id, endpoint, date_param)
        if endpoint in self.reports_required_accounting_type:

            accrual_url = url + "&accounting_method=Accrual"
            cash_url = url + "&accounting_method=Cash"

            results = self._request(accrual_url, params)
            self.data = results

            results_2 = self._request(cash_url, params)
            self.data_2 = results_2

        else:

            results = self._request(url)
            self.data = results
