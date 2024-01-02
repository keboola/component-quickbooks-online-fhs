import logging
import csv
import os
import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import backoff

from mapping import Mapping
from client import QuickbooksClient, QuickBooksClientException
from report_mapping import ReportMapping

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException  # noqa

URL_SUFFIXES = {"US": ".keboola.com",
                "EU": ".eu-central-1.keboola.com",
                "AZURE-EU": ".north-europe.azure.keboola.com",
                "CURRENT_STACK": os.environ.get('KBC_STACKID', 'connection.keboola.com').replace('connection', '')}

# configuration variables
KEY_COMPANY_ID = 'companyid'
KEY_ENDPOINTS = 'endpoints'
KEY_REPORTS = 'reports'
GROUP_DATE_SETTINGS = 'date_settings'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
KEY_GROUP_DESTINATION = 'destination'
KEY_LOAD_TYPE = 'load_type'
KEY_SUMMARIZE_COLUMN_BY = 'summarize_column_by'
KEY_SANDBOX = 'sandbox'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINTS, KEY_GROUP_DESTINATION]

# QuickBooks Parameters
BASE_URL = "https://quickbooks.api.intuit.com"

ALLOWED_BRANCHES = ["683762"]
ALLOWED_PROJECTS = ["9525"]


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.incremental = None
        self.refresh_token = None
        self.access_token = None

        if self.environment_variables.branch_id not in ALLOWED_BRANCHES:
            raise UserException(f"This component uses Keboola API to store the statefile. "
                                f"Running is allowed only in branches {ALLOWED_BRANCHES}, "
                                f"detected branch: {self.environment_variables.branch_id}")

        if self.environment_variables.project_id not in ALLOWED_PROJECTS:
            raise UserException(f"This component uses Keboola API to store the statefile. "
                                f"Running is allowed only in projects {ALLOWED_PROJECTS}, "
                                f"detected project_id: {self.environment_variables.project_id}")

    def run(self):

        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        start_date = None
        end_date = None

        oauth = self.configuration.oauth_credentials
        self.refresh_token, self.access_token = self.get_tokens(oauth)

        params_company_id = self.configuration.parameters.get(KEY_COMPANY_ID, None)

        in_tables = self.get_input_tables_definitions()
        if in_tables:
            cfg_table = in_tables[0]
        else:
            cfg_table = False

        if cfg_table:
            self.validate_inputs(cfg_table, params_company_id)
            try:
                self.input_table_run(cfg_table, oauth, sandbox, params_company_id)
            except QuickBooksClientException as e:
                raise UserException(f"Component failed during run: {e}") from e
        else:
            try:
                self.no_input_table_run(start_date, end_date, self.refresh_token, self.access_token, oauth, sandbox)
            except QuickBooksClientException as e:
                raise UserException(f"Component failed during run: {e}") from e

        self.write_state_file({
            "tokens":
                {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                 "#refresh_token": self.refresh_token,
                 "#access_token": self.access_token}
        })

    @staticmethod
    def validate_company_id(company_id: str) -> None:
        if ' ' in company_id or '.' in company_id:
            raise UserException("The company_id parameter should not contain any spaces or dots.")

    def validate_inputs(self, cfg_table, params_company_id: str) -> None:
        self.validate_company_id(params_company_id)
        with open(cfg_table.full_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            for row in rows:
                pk = row["PK"]
                if pk != params_company_id:
                    raise UserException(f"company_id from params: {params_company_id} does not match "
                                        f"with company_id provided in input table: {pk}.")

    def no_input_table_run(self, start_date, end_date, refresh_token, access_token, oauth, sandbox):
        logging.info("No input table detected. The component will run with parameters set in config.")
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Input parameters
        endpoints = params.get(KEY_ENDPOINTS)
        reports = params.get(KEY_REPORTS, [])
        company_id = params.get(KEY_COMPANY_ID)
        endpoints.extend(reports)

        if params.get(GROUP_DATE_SETTINGS):
            date_settings = params.get(GROUP_DATE_SETTINGS)
            start_date = date_settings.get(KEY_START_DATE, None)
            end_date = date_settings.get(KEY_END_DATE, None)

        start_date = self.process_date(start_date)
        end_date = self.process_date(end_date)

        logging.info(f'Processing Company ID: {company_id}')

        if params.get("sandbox"):
            sandbox = True
            logging.info("Sandbox environment enabled.")

        destination_params = params.get(KEY_GROUP_DESTINATION)
        if destination_params.get(KEY_LOAD_TYPE, False) == "incremental_load":
            self.incremental = True
        else:
            self.incremental = False
        logging.debug(f"Load type incremental set to: {self.incremental}")

        summarize_column_by = params.get(KEY_SUMMARIZE_COLUMN_BY) if params.get(
            KEY_SUMMARIZE_COLUMN_BY) else None

        quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=refresh_token,
                                            access_token=access_token, oauth=oauth, sandbox=sandbox)
        if not sandbox:
            self.process_oauth_tokens(quickbooks_param)

        # Fetching reports for each configured endpoint
        for endpoint in endpoints:
            self.process_endpoint(endpoint, quickbooks_param, start_date, end_date, summarize_column_by)

        self.refresh_token, self.access_token = quickbooks_param.refresh_token, quickbooks_param.access_token

    def input_table_run(self, cfg_table, oauth, sandbox, params_company_id: str):
        _endpoints = self.configuration.parameters.get("endpoints", [])
        with open(cfg_table.full_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)  # not memory efficient, but we are working with small input table
            if len(rows) == 0:
                logging.info("Not rows in input table detected, the component will process selected endpoints only.")
                quickbooks_param = QuickbooksClient(company_id=params_company_id, refresh_token=self.refresh_token,
                                                    access_token=self.access_token, oauth=oauth, sandbox=sandbox)
                if not sandbox:
                    self.process_oauth_tokens(quickbooks_param)
                for endpoint in _endpoints:
                    self.process_endpoint(endpoint, quickbooks_param, start_date=None, end_date=None,
                                          summarize_column_by=None)

                self.refresh_token, self.access_token = quickbooks_param.refresh_token, quickbooks_param.access_token
            else:
                for row in rows:
                    logging.debug(f"Processing row: {row}")
                    company_id = row["PK"]
                    endpoints = _endpoints + [row["report"]]
                    start_date = row["start_date"]
                    end_date = row["end_date"]
                    self.incremental = True
                    summarize_column_by = row["segment_data_by"] or None

                    quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=self.refresh_token,
                                                        access_token=self.access_token, oauth=oauth, sandbox=sandbox)

                    if not sandbox:
                        self.process_oauth_tokens(quickbooks_param)

                    # Fetching reports for each configured endpoint
                    for endpoint in endpoints:
                        self.process_endpoint(endpoint, quickbooks_param, start_date, end_date, summarize_column_by)

                    self.refresh_token, self.access_token = quickbooks_param.refresh_token, \
                        quickbooks_param.access_token

    def process_oauth_tokens(self, client) -> None:
        """Uses Quickbooks client to get new tokens and saves them using API if they have changed since the last run."""
        new_refresh_token, new_access_token = client.get_new_refresh_token()
        if self.refresh_token != new_refresh_token:
            self.save_new_oauth_tokens(new_refresh_token, new_access_token)

            # We also save new tokens to class vars, so we can save them unencrypted if case statefile update fails
            # in update_config_state() method.
            self.refresh_token = new_refresh_token
            self.access_token = new_access_token

    def save_new_oauth_tokens(self, refresh_token: str, access_token: str) -> None:
        logging.debug("Saving new tokens to state using Keboola API.")

        try:
            encrypted_refresh_token = self.encrypt(refresh_token)
            encrypted_access_token = self.encrypt(access_token)
        except requests.exceptions.RequestException:
            logging.warning("Encrypt API is unavailable. Skipping token save at the beginning of the run.")
            return

        new_state = {
            "component": {
                "tokens":
                    {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     "#refresh_token": encrypted_refresh_token,
                     "#access_token": encrypted_access_token}
            }}
        try:
            self.update_config_state(region="CURRENT_STACK",
                                     component_id=self.environment_variables.component_id,
                                     configurationId=self.environment_variables.config_id,
                                     state=new_state,
                                     branch_id=self.environment_variables.branch_id)
        except requests.exceptions.RequestException:
            logging.warning("Storage API (update config state)"
                            "is unavailable. Skipping token save at the beginning of the run.")
            return

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        url = "https://encryption.keboola.com/encrypt"
        params = {
            "componentId": self.environment_variables.component_id,
            "projectId": self.environment_variables.project_id,
            "configId": self.environment_variables.config_id
        }
        headers = {"Content-Type": "text/plain"}

        response = requests.post(url,
                                 data=token,
                                 params=params,
                                 headers=headers)
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def update_config_state(self, region, component_id, configurationId, state, branch_id='default'):
        if not branch_id:
            branch_id = 'default'

        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/branch/{branch_id}' \
              f'/components/{component_id}/configs/' \
              f'{configurationId}/state'

        parameters = {'state': json.dumps(state)}
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'X-StorageApi-Token': self._get_storage_token()}
        response = requests.put(url,
                                data=parameters,
                                headers=headers)
        response.raise_for_status()

    def process_endpoint(self, endpoint, quickbooks_param, start_date, end_date, summarize_column_by):

        if endpoint == "ProfitAndLossQuery":
            self.process_pnl_report(quickbooks_param=quickbooks_param, start_date=start_date, end_date=end_date,
                                    summarize_column_by=summarize_column_by)
            return

        if "**" in endpoint:
            endpoint = endpoint.split("**")[0]
            report_api_bool = True
        else:
            endpoint = endpoint
            report_api_bool = False

        self.fetch(quickbooks_param=quickbooks_param, endpoint=endpoint, report_api_bool=report_api_bool,
                   start_date=start_date, end_date=end_date)

        logging.debug("Parsing API results...")
        input_data = quickbooks_param.data

        if len(input_data) == 0:
            pass
        else:
            logging.debug(
                "Report API Template Enable: {0}".format(report_api_bool))
            if report_api_bool:
                if endpoint == "CustomQuery":
                    # Not implemented
                    ReportMapping(endpoint=endpoint, data=input_data,
                                  query=start_date)
                else:
                    if endpoint in quickbooks_param.reports_required_accounting_type:
                        input_data_2 = quickbooks_param.data_2
                        ReportMapping(endpoint=endpoint, data=input_data, accounting_type="accrual")
                        ReportMapping(endpoint=endpoint, data=input_data_2, accounting_type="cash")
                    else:
                        ReportMapping(endpoint=endpoint, data=input_data)
            else:
                Mapping(endpoint=endpoint, data=input_data)

    def get_tokens(self, oauth):

        try:
            refresh_token = oauth["data"]["refresh_token"]
            access_token = oauth["data"]["access_token"]
        except TypeError:
            raise UserException("OAuth data is not available.")

        statefile = self.get_state_file()
        if statefile.get("tokens", {}).get("ts"):
            ts_oauth = datetime.datetime.strptime(oauth["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts_statefile = datetime.datetime.strptime(statefile["tokens"]["ts"], "%Y-%m-%dT%H:%M:%S.%fZ")

            if ts_statefile > ts_oauth:
                refresh_token = statefile["tokens"].get("#refresh_token")
                access_token = statefile["tokens"].get("#access_token")
                logging.debug("Loaded tokens from statefile.")
            else:
                logging.debug("Using tokens from oAuth.")
        else:
            logging.warning("No timestamp found in statefile. Using oAuth tokens.")

        return refresh_token, access_token

    def process_pnl_report(self, quickbooks_param, start_date, end_date, summarize_column_by):
        results_cash = []
        results_accrual = []

        def save_result(class_name, name, value, obj_type, obj_group, method):
            res_dict = {
                "class": class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group,
                "start_date": start_date,
                "end_date": end_date
            }
            if method == "cash":
                results_cash.append(res_dict)
            elif method == "accrual":
                results_accrual.append(res_dict)
            else:
                raise UserException(f"Unknown accounting method: {method}")

        def process_coldata(obj, obj_type, obj_group, method):
            col_data = obj["ColData"]
            name = col_data[0]["value"]
            value = col_data[1]["value"]
            save_result(summary_name, name, value, obj_type, obj_group, method)

        def process_object(obj, class_name, method):
            obj_type = obj.get("type", "")
            obj_group = obj.get("group", "")

            if "ColData" in obj:
                process_coldata(obj, obj_type, obj_group, method)

            if "Header" in obj:
                header_name = obj["Header"]["ColData"][0]["value"]
                header_value = obj["Header"]["ColData"][1]["value"]
                save_result(class_name, header_name, header_value, obj_type, obj_group, method)

            if "Summary" in obj:
                summary_name = obj["Summary"]["ColData"][0]["value"]
                summary_value = obj["Summary"]["ColData"][1]["value"]
                save_result(class_name, summary_name, summary_value, obj_type, obj_group, method)

            if "Rows" in obj:
                inner_objects = obj["Rows"]["Row"]
                for inner_object in inner_objects:
                    process_object(inner_object, class_name, method)

        summary_names = ["Total"]
        summary_ids = [None]
        params = {}

        valid_object_summaries = ["Class", "Department", "Total"]
        if summarize_column_by not in valid_object_summaries:
            raise UserException(f"The component can process ProfitAndLossQuery report only for"
                                f" {valid_object_summaries}.")

        if summarize_column_by != "Total":
            self.fetch(quickbooks_param=quickbooks_param,
                       endpoint="CustomQuery",
                       report_api_bool=True,
                       start_date=start_date,
                       end_date=end_date,
                       query=f"select * from {summarize_column_by}")

            query_result = quickbooks_param.data

            summary_names = [c["Name"] for c in query_result.get(summarize_column_by, []) if c.get("Name")]
            summary_ids = [c["Id"] for c in query_result.get(summarize_column_by, []) if c.get("Id")]

            logging.debug(f"Found summary categories: {summary_names}")

            if not summary_names:
                raise UserException(f"API returned no {summarize_column_by}. Please make sure you have relevant "
                                    f"objects set up in your Quickbooks account.")
            else:
                logging.debug(f"Summarize is: {summarize_column_by}")
                if summarize_column_by:
                    if summarize_column_by == "Class":
                        params["summarize_column_by"] = "Classes"
                    elif summarize_column_by == "Department":
                        params["summarize_column_by"] = "Departments"
                    else:
                        raise UserException(f"Cannot Group by {summarize_column_by}")

        for summary_name, summary_id in zip(summary_names, summary_ids):
            logging.debug(f"Processing summary: {summary_names} with id {summary_ids}")

            if summarize_column_by in ["Class", "Department"]:
                # filter results by Classes or Departments
                params[str(summarize_column_by).lower()] = summary_id
                logging.debug(f"Filtering for pnl report is set to: {summarize_column_by}")
            else:
                logging.debug("Filtering for pnl report is not set.")

            self.fetch(quickbooks_param=quickbooks_param,
                       endpoint="ProfitAndLoss",
                       report_api_bool=True,
                       start_date=start_date,
                       end_date=end_date,
                       query="",
                       params=params)

            summarize_by = quickbooks_param.data['Header'].get("SummarizeColumnsBy", False)

            if not summarize_by:
                # This part is currently not used since we always group by Class, Department or Total

                report_accrual = quickbooks_param.data['Rows']['Row']
                report_cash = quickbooks_param.data_2['Rows']['Row']

                for obj in report_cash:
                    process_object(obj, summary_name, method="cash")
                for obj in report_accrual:
                    process_object(obj, summary_name, method="accrual")

            else:
                report_accrual_data = quickbooks_param.data
                report_cash_data = quickbooks_param.data_2

                header = quickbooks_param.data['Header']
                summarize_by = header['SummarizeColumnsBy']
                currency = header['Currency']

                results_cash.append(self.preprocess_dict(report_cash_data,
                                                         summary_name,
                                                         summarize_by=summarize_by,
                                                         currency=currency,
                                                         start_date=start_date,
                                                         end_date=end_date))

                results_accrual.append(self.preprocess_dict(report_accrual_data,
                                                            summary_name,
                                                            summarize_by=summarize_by,
                                                            currency=currency,
                                                            start_date=start_date,
                                                            end_date=end_date))

        """
        # This is here in case we will ever need to do reports that are not summarized
        if summarize_by:
            suffix = "_" + str(summarize_by)
        else:
            suffix = ""
        """

        self.save_pnl_report_to_csv(table_name="ProfitAndLossQuery_cash.csv", results=results_cash)
        self.save_pnl_report_to_csv(table_name="ProfitAndLossQuery_accrual.csv", results=results_accrual)

    @staticmethod
    def preprocess_dict(obj, class_name, summarize_by, currency, start_date, end_date):
        results = []

        rows = obj['Rows']['Row']
        cols = obj['Columns']['Column']
        group_by = []
        for col in cols:
            group_by.append(col['ColTitle'])

        def save_result(_class_name, name, value, obj_type, obj_group, category_name="", category_id=""):
            res_dict = {
                "class": _class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group,
                "category_name": category_name,
                "category_id": category_id,
                "start_date": start_date,
                "end_date": end_date,
                "summarize_by": summarize_by,
                "currency": currency
            }
            results.append(res_dict)

        def process_coldata(obj, obj_type, obj_group):
            col_data = obj["ColData"]
            category_name = col_data[0].get("value", "")
            category_id = col_data[0].get("id", "")
            for name, val in zip(group_by, col_data):
                if name:
                    save_result(class_name, name, val['value'], obj_type, obj_group, category_name, category_id)

        def process_object(obj, class_name):
            obj_type = obj.get("type", "")
            obj_group = obj.get("group", "")

            if "ColData" in obj:
                process_coldata(obj, obj_type, obj_group)

            if "Header" in obj:
                header_name = obj["Header"]["ColData"][0]["value"]
                header_value = obj["Header"]["ColData"][1]["value"] if len(obj["Header"]["ColData"]) > 1 else ""
                save_result(class_name, header_name, header_value, obj_type, obj_group)

            if "Summary" in obj:
                summary_name = obj["Summary"]["ColData"][0]["value"]
                summary_value = obj["Summary"]["ColData"][1]["value"] if len(obj["Summary"]["ColData"]) > 1 else ""
                save_result(class_name, summary_name, summary_value, obj_type, obj_group)

            if "Rows" in obj:
                inner_objects = obj["Rows"]["Row"]
                for inner_object in inner_objects:
                    process_object(inner_object, class_name)

        for row in rows:
            process_object(row, class_name)

        return results

    def save_pnl_report_to_csv(self, table_name: str, results: list):

        logging.debug(f"Saving pnl_report results to {table_name}.")

        pk = ["class", "name", "obj_type", "category_id", "start_date", "end_date"]
        columns = ["class", "name", "value", "obj_type", "obj_group", "category_name", "category_id",
                   "start_date", "end_date", "summarize_by", "currency"]

        table_def = self.create_out_table_definition(table_name, primary_key=pk, incremental=self.incremental)

        file_exists = os.path.isfile(table_def.full_path)

        with open(table_def.full_path, 'a', newline='') as csvfile:
            wr = csv.DictWriter(csvfile, fieldnames=columns)
            if not file_exists:
                wr.writeheader()
            for result in results:
                wr.writerows(result)

        self.write_manifest(table_def)

    @staticmethod
    def fetch(quickbooks_param, endpoint, report_api_bool, start_date=None, end_date=None, query="", params=None):
        logging.debug(f"Fetching endpoint {endpoint} with date rage: {start_date} - {end_date}")
        try:
            quickbooks_param.fetch(
                endpoint=endpoint,
                report_api_bool=report_api_bool,
                start_date=start_date,
                end_date=end_date,
                query=query if query else "",
                params=params
            )
        except QuickBooksClientException as e:
            raise UserException(e) from e

    @staticmethod
    def process_date(dt):
        """Checks if date is in valid format. If not, raises UserException. If None, returns None"""
        if not dt:
            return None

        dt_format = '%Y-%m-%d'
        today = datetime.date.today()
        if dt == "PrevMonthStart":
            result = today.replace(day=1) - relativedelta(months=1)
        elif dt == "PrevMonthEnd":
            result = today.replace(day=1) - relativedelta(days=1)
        else:
            try:
                datetime.date.fromisoformat(dt)
            except ValueError:
                raise UserException(f"Date {dt} is invalid. Valid types are: "
                                    f"PrevMonthStart, PrevMonthEnd or YYYY-MM-DD")
            return dt
        return result.strftime(dt_format)

    def _get_storage_token(self) -> str:
        token = self.configuration.parameters.get('#storage_token') or self.environment_variables.token
        if not token:
            raise UserException("Cannot retrieve storage token from env variables and/or config.")
        return token


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
