#!/usr/bin/env python3

import argparse
import functools
import io
import os
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources

from jsonschema import validate
import singer

import httplib2

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

MAX_RECORDS = 50000

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer Sheets Target'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-singer-target.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def get_spreadsheet(service, spreadsheet_id):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

def get_values(service, spreadsheet_id, range):
    return service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range).execute()

def batch_update_cells(service, spreadsheet_id, updateCells):
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            'requests':[
                {
                    'updateCells': updateCells
                }
            ]
        }).execute()

def add_sheet(service, spreadsheet_id, title):
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            'requests':[
                {
                    'addSheet': {
                    'properties': {
                        'title': title,
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 26
                        }
                    }
                    }
                }
            ]
        }).execute()

def append_to_sheet(service, spreadsheet_id, range, values):
    return service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption='USER_ENTERED',
        body={'values': [values]}).execute()


def append_to_sheet_multi(service, spreadsheet_id, range, values):
    return service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption='USER_ENTERED',
        body={'values': values}).execute()

def clear_range(service, spreadsheet_id, range):
    return service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range,
        body={}).execute()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

STREAM_MAP = {}
def stream_tab_map(stream):
    global STREAM_MAP
    if stream in STREAM_MAP:
        #logger.info('found ' + STREAM_MAP[stream])
        return STREAM_MAP[stream]
    else:
        #logger.info('not found {}, {}'.format(stream, STREAM_MAP))
        return stream

def get_sheet_id(spreadsheet, sheetname):
    found_sheet_id = None
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheetname:
            found_sheet_id = sheet['properties']['sheetId']
            break
    if found_sheet_id == None:
        raise Exception('Data sheet {} not found in document'.format(sheetname))
    return found_sheet_id

PIVOT_TABLES = [
    {
        'name': 'By asdf',
        'datasheet': 'Raw Data',
        'numcols': 99,
    },
    #{
    #    'name': 'By Commit',
    #    'datasheet': 'Raw Data',
    #    'numcols': 99,
    #},
    #{
    #    'name': 'By Author',
    #    'datasheet': 'Raw Data',
    #    'numcols': 99,
    #}
]

def init_pivot_tables(service, spreadsheet, should_replace = True):
    for table in PIVOT_TABLES:
        init_pivot_table(service, spreadsheet, table, should_replace)

def init_pivot_table(service, spreadsheet, table, should_replace):
    matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == table['name']]
    if matching_sheet and not should_replace:
        return
    elif matching_sheet:
        # Clear the sheet
        range_to_clear = "{}!A1:ZZZ999999".format(table['name'])
        clear_range(service, spreadsheet['spreadsheetId'], range_to_clear)
    else:
        # Create the sheet
        add_sheet(service, spreadsheet['spreadsheetId'], table['name'])
        # Refresh this
        spreadsheet = get_spreadsheet(service, spreadsheet['spreadsheetId'])
        matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] ==
            table['name']]

    pivot_sheet_id = get_sheet_id(spreadsheet, table['name'])
    data_sheet_id = get_sheet_id(spreadsheet, table['datasheet'])

    # Now, put a pivot table in cell A1
    tableDef = {
        'rows': {
            'values': [
                {
                    'pivotTable': {
                        'source': {
                            'sheetId': data_sheet_id,
                            'startRowIndex': 0,
                            'startColumnIndex': 0,
                            'endRowIndex': MAX_RECORDS,
                            'endColumnIndex': table['numcols']
                        },
                        'rows': [
                            {
                                'sourceColumnOffset': 1,
                                'showTotals': True,
                                'sortOrder': 'ASCENDING',
                            },
                        ],
                        'values': [
                            {
                                'summarizeFunction': 'COUNTA',
                                'sourceColumnOffset': 4
                            }
                        ],
                        'valueLayout': 'HORIZONTAL'
                        'filterSpecs'
                    }
                }
            ]
        },
        'start': {
            'sheetId': pivot_sheet_id,
            'rowIndex': 1,
            'columnIndex': 0
        },
        'fields': 'pivotTable'
    }

    batch_update_cells(service, spreadsheet['spreadsheetId'], tableDef)


def persist_lines(service, spreadsheet, lines, clear_existing_lines):
    state = None
    schemas = {}
    key_properties = {}

    headers_by_stream = {}

    lines_by_stream = {}

    logger.info('reading input records')
    recordCount = 0
    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            recordCount += 1
            if recordCount > 10:
                break
            if recordCount % 1000 == 0:
                logger.info('{} input records received...'.format(recordCount))
            if recordCount > MAX_RECORDS:
                raise Exception('Maximum record count of 50,000 exceeded')

            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]
            validate(msg.record, schema)
            flattened_record = flatten(msg.record)

            sheet_name = stream_tab_map(msg.stream)

            matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == sheet_name]
            new_sheet_needed = len(matching_sheet) == 0
            range_name = "{}!A1:ZZZ".format(sheet_name)
            append = functools.partial(append_to_sheet, service, spreadsheet['spreadsheetId'], range_name)

            if new_sheet_needed:
                add_sheet(service, spreadsheet['spreadsheetId'], sheet_name)
                spreadsheet = get_spreadsheet(service, spreadsheet['spreadsheetId']) # refresh this for future iterations
                headers_by_stream[msg.stream] = list(flattened_record.keys())
                append(headers_by_stream[msg.stream])

            elif msg.stream not in headers_by_stream:
                first_row = get_values(service, spreadsheet['spreadsheetId'], range_name + '1')
                if 'values' in first_row:
                    headers_by_stream[msg.stream] = first_row.get('values', None)[0]
                else:
                    headers_by_stream[msg.stream] = list(flattened_record.keys())
                    append(headers_by_stream[msg.stream])

                # Clear all rows after the first row in this sheet
                if clear_existing_lines:
                    range_to_clear = "{}!A2:ZZZ50000".format(sheet_name)
                    clear_range(service, spreadsheet['spreadsheetId'], range_to_clear)

            if msg.stream not in lines_by_stream:
                lines_by_stream[msg.stream] = []

            lines_by_stream[msg.stream].append(flattened_record)
        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value
        elif isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties
        else:
            logger.info("Unrecognized message {}".format(msg))

    logger.info('{} total input records'.format(recordCount))

    for item in lines_by_stream.items():
        msg_stream = item[0]
        sheet_name = stream_tab_map(msg_stream)
        range_name = "{}!A1:ZZZ".format(sheet_name)
        append = functools.partial(append_to_sheet_multi, service, spreadsheet['spreadsheetId'],
            range_name)
        records = item[1]

        logger.info("appending {} records for stream '{}'', sheet '{}'".format(len(records), msg_stream, sheet_name))

        inputRecords = []
        for r in records:
            inputRecords.append([r.get(x, None) for x in headers_by_stream[msg_stream]])
        result = append(inputRecords) # order by actual headers found in sheet

    return state


def collect():
    try:
        version = pkg_resources.get_distribution('target-gsheet').version
        conn = http.client.HTTPSConnection('collector.stitchdata.com', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-gsheet',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    global STREAM_MAP
    with open(flags.config) as input:
        config = json.load(input)

    if not config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheet = get_spreadsheet(service, config['spreadsheet_id'])

    logger.info(config)
    if 'schema_tab_map' in config:
        STREAM_MAP = config['schema_tab_map']
        logger.info(STREAM_MAP)

    clear_existing_lines = False
    if 'clear_existing_lines' in config:
        clear_existing_lines = config['clear_existing_lines']


    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    init_pivot_tables(service, spreadsheet)
    state = None
    state = persist_lines(service, spreadsheet, input, clear_existing_lines)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
