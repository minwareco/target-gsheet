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

def batch_requests(service, spreadsheet_id, requestList):
    #logger.info(requestList)
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            'requests': requestList
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

def column_width_update(sheet_id, colIdx, widthPx, numCols=1):
    return {
        'updateDimensionProperties': {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": colIdx,
                "endIndex": colIdx + numCols
            },
            "properties": {
                "pixelSize": widthPx
            },
            "fields": "pixelSize"
        }
    }

def freeze_columns_rows(sheet_id, numCols=1, numRows=1):
    return {
        'updateSheetProperties': {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {
                    'frozenColumnCount': numCols,
                    'frozenRowCount': numRows
                }
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
        }
    }

def merge_cells(sheet_id, startCol=0, startRow=0, numCols=1, numRows=1):
    return {
        'mergeCells': {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": startRow,
                "endRowIndex": startRow + numRows,
                "startColumnIndex": startCol,
                "endColumnIndex": startCol + numCols,
            },
            "mergeType": "MERGE_ALL",
        }
    }

FORMAT_DECIMAL3 = '#####0.000'
FORMAT_PERCENT1 = '##0.0%'
def format_cells(sheet_id, format, formatType='NUMBER', startCol=0, startRow=0, numCols=1, numRows=1):
    return {
        'repeatCell': {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": startRow,
                "endRowIndex": startRow + numRows,
                "startColumnIndex": startCol,
                "endColumnIndex": startCol + numCols,
            },
            'cell': {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": formatType, # NUMBER, PERCENT, DATE, TEXT, etc., see:
                        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#NumberFormat
                        "pattern": format, # See:
                        # https://developers.google.com/sheets/api/guides/formats
                    },
                },
            },
            "fields": "userEnteredFormat.numberFormat",
        },
    }

def set_column_left_border(sheet_id, startCol=0, startRow=0, numRows=1, borderStyle='SOLID'):
    return {
        'updateBorders': {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": startRow,
                "endRowIndex": startRow + numRows,
                "startColumnIndex": startCol,
                "endColumnIndex": startCol + 1,
            },
            "left": {
                "style": borderStyle,
                "width": 1,
                "color": COLOR_BLACK,
            },
        },
    }

COLOR_BLACK = {'red': 0, 'green': 0, 'blue': 0}
COLOR_WHITE = {'red': 1, 'green': 1, 'blue': 1}
COLOR_BLUE_80 = {'red': .8, 'green': .8, 'blue': 1}
COLOR_GREEN_80 = {'red': .8, 'green': 1, 'blue': .8}
COLOR_GREEN_0 = {'red': 0, 'green': .7, 'blue': 0}
COLOR_YELLOW_0 = {'red': .85, 'green': .85, 'blue': 0.2}
COLOR_RED_70 = {'red': 1, 'green': .7, 'blue': .7}
COLOR_YELLOW_70 = {'red': 1, 'green': 1, 'blue': .7}
COLOR_MAGENTA_70 = {'red': 1, 'green': .7, 'blue': 1}
COLOR_CYAN_70 = {'red': .7, 'green': 1, 'blue': 1}
ALTERNATING_COLORS = [
    COLOR_BLUE_80,
    COLOR_GREEN_80,
    COLOR_MAGENTA_70,
    COLOR_CYAN_70,
]

def update_cells(sheet_id, values, startCol=0, startRow=0, bgColor=COLOR_WHITE, halign='CENTER'):
    # Nest in lists if not explicitly wrapped
    if not isinstance(values, list):
        values = [values]
    if not isinstance(values[0], list):
        values = [values]
    
    rows = []
    for row in values:
        rowObject = {
            'values': []
        }
        for colVal in row:
            rowObject['values'].append({
                'userEnteredValue': {'stringValue': colVal},
                'userEnteredFormat': {
                    # TODO LATER MAYBE: use theme colors instead?
                    'backgroundColor': bgColor,
                    'horizontalAlignment': halign, # LEFT, CENTER, or RIGHT
                }
            })
        rows.append(rowObject)

    return {
        'updateCells': {
            'rows': rows,
            'fields': 'userEnteredValue,userEnteredFormat',
            'start': {
                'sheetId': sheet_id,
                'rowIndex': startRow,
                'columnIndex': startCol,
            },
        },
    }

def conditional_format(sheet_id, startCol=0, startRow=0, numCols=1, numRows=1,
        maxColor=COLOR_GREEN_0, percentile=False):
    gradientRule = {
        "minpoint": {
            "color": COLOR_WHITE,
            "type": "NUMBER",
            "value": '0',
        },
    }
    if percentile:
        gradientRule['maxpoint'] = {
            "color": maxColor,
            "type": "PERCENTILE",
            "value": '90',
        }
    else:
        gradientRule['maxpoint'] = {
            "color": maxColor,
            "type": "MAX",
        }
        '''
        midColor = {
            'red': (1 + maxColor['red']) / 2,
            'green': (1 + maxColor['green']) / 2,
            'blue': (1 + maxColor['blue']) / 2,
        }
        gradientRule['midpoint'] = {
            "color": midColor,
            "type": "PERCENT",
            "value": '25',
        }
        '''
    
    return {
        'addConditionalFormatRule': {
            'rule': {
                'ranges': [
                    {
                        'sheetId': sheet_id,
                        'startRowIndex': startRow,
                        'startColumnIndex': startCol,
                        'endRowIndex': startRow + numRows,
                        'endColumnIndex': startCol + numCols,
                    },
                ],
                "gradientRule": gradientRule,
            },
            'index': 0,
        },
    }
    
def delete_conditional_format(sheet_id, idx):
    return {
        "deleteConditionalFormatRule": {
            "sheetId": sheet_id,
            "index": idx,
        }
    }

VAL_GROUPS = {
    'LONGEVITY': [
        {
            'heading': '',
            'colWidthPx': 90,
            'formatType': 'NUMBER',
            'format': FORMAT_DECIMAL3,
            'maxColor': COLOR_GREEN_0,
            'colorPercentile': True,
            'values': [
                {
                    'label': 'Commit Days',
                    'formula': '=LINES_ADDED'
                }
            ]
        },
        {
            'heading': 'Time from main branch commit to removal/replacement',
            'colWidthPx': 75,
            'formatType': 'PERCENT',
            'format': FORMAT_PERCENT1,
            'maxColor': COLOR_YELLOW_0,
            'values': [
                {
                    'label': 'Immediate',
                    'formula': '=REMOVED_IMMEDIATE/LINES_ADDED'
                },
                {
                    'label': '< 5 Minutes',
                    'formula': '=REMOVED_WITHIN_5MINUTES/LINES_ADDED'
                },
                {
                    'label': '< 1 Hour',
                    'formula': '=REMOVED_WITHIN_1HOUR/LINES_ADDED'
                },
                {
                    'label': '< 1 Day',
                    'formula': '=REMOVED_WITHIN_1DAY/LINES_ADDED'
                },
                {
                    'label': '< 1 Week',
                    'formula': '=REMOVED_WITHIN_1WEEK/LINES_ADDED'
                },
                {
                    'label': '< 30 Days',
                    'formula': '=REMOVED_WITHIN_30DAYS/LINES_ADDED'
                },
                {
                    'label': '< 60 Days',
                    'formula': '=REMOVED_WITHIN_60DAYS/LINES_ADDED'
                },
                {
                    'label': '< 90 Days',
                    'formula': '=REMOVED_WITHIN_90DAYS/LINES_ADDED'
                },
                {
                    'label': '< 120 Days',
                    'formula': '=REMOVED_WITHIN_120DAYS/LINES_ADDED'
                },
                {
                    'label': '< 1 Year',
                    'formula': '=REMOVED_WITHIN_1YEAR/LINES_ADDED'
                },
                {
                    'label': '>= 1 Year',
                    'formula': '=REMOVED_AFTER_1YEAR/LINES_ADDED'
                },
            ]
        },
        {
            'heading': 'Survival rate',
            'colWidthPx': 100,
            'formatType': 'PERCENT',
            'format': FORMAT_PERCENT1,
            'maxColor': COLOR_GREEN_0,
            'values': [
                {
                    'label': '1-Year Survival',
                    'formula': '=1-REMOVED_LT_1YEAR/LINES_ADDED'
                },
                {
                    'label': 'Still Surviving',
                    'formula': '=LINES_REMAINING/LINES_ADDED'
                },
            ],
        },
        {
            'heading': '',
            'colWidthPx': 85,
            'values': [
                {
                    'label': 'Lines Added',
                    'formula': '=ORIG_LINES_ADDED'
                },
            ],
        },
    ]
}

PIVOT_TABLES = [
    {
        'name': 'By Author',
        'datasheet': 'Raw Data',
        'numcols': 99,
        'rows': {
            'label': 'Author Email',
            'sourceColumnOffset': 1,
            'sortOrder': 'DESCENDING',
            'showTotals': True,
            'valueBucket': {
                'valuesIndex': 0
            },
        },
        'firstColWidthPx': 280,
        'rowcol': 1,
        'rowlabel': 'Author Email',
        'valueGroups': VAL_GROUPS['LONGEVITY'],
    },
    {
        'name': 'By Commit',
        'datasheet': 'Raw Data',
        'numcols': 99,
        'rows': {
            'label': 'Commit Hash',
            'sourceColumnOffset': 4,
            'sortOrder': 'DESCENDING',
            'showTotals': True,
            'valueBucket': {
                'valuesIndex': len(VAL_GROUPS['LONGEVITY'][0]['values']) +
                    len(VAL_GROUPS['LONGEVITY'][1]['values']) +
                    len(VAL_GROUPS['LONGEVITY'][2]['values'])
            },
        },
        'firstColWidthPx': 300,
        'rowcol': 1,
        'rowlabel': 'Commit Hash',
        'valueGroups': VAL_GROUPS['LONGEVITY'],
    },
    {
        'name': 'By Date',
        'datasheet': 'Raw Data',
        'numcols': 99,
        'rows': {
            'label': 'Week Start',
            'sourceColumnOffset': 19,
            'sortOrder': 'ASCENDING',
            'showTotals': True,
            'valueBucket': {
                      "buckets": [
                        {
                          "stringValue": "WEEK_START"
                        }
                      ]
            },
        },
        'firstColWidthPx': 100,
        'rowcol': 1,
        'rowlabel': 'Week Start',
        'valueGroups': VAL_GROUPS['LONGEVITY'],
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

    pivot_sheet = matching_sheet[0]
    data_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == table['datasheet']][0]

    pivot_sheet_id = pivot_sheet['properties']['sheetId']
    data_sheet_id = data_sheet['properties']['sheetId']
    data_grid_properties = data_sheet['properties']['gridProperties']

    requests = []
    requests.append(column_width_update(pivot_sheet_id, 0, table['firstColWidthPx']))
    requests.append(freeze_columns_rows(pivot_sheet_id, 1, 2))
    ct = 0
    if 'conditionalFormats' in pivot_sheet:
        for fmt in pivot_sheet['conditionalFormats']:
            requests.append(delete_conditional_format(pivot_sheet_id, 0))
            ct += 1

    # Now, put a pivot table in cell A1
    valueList = []
    totalColCount = 0
    groupColorIdx = 0
    formatNumRows = data_grid_properties['rowCount']
    for valGroup in table['valueGroups']:
        heading = valGroup['heading']
        colCount = len(valGroup['values'])
        requests.append(column_width_update(pivot_sheet_id, 1 + totalColCount,
            valGroup['colWidthPx'], colCount))
        if colCount > 1:
            requests.append(merge_cells(pivot_sheet_id, startCol=1+totalColCount, numCols=colCount))
        requests.append(set_column_left_border(pivot_sheet_id, startCol=1+totalColCount,
            numRows=formatNumRows))
        if heading:
            bgColor = ALTERNATING_COLORS[groupColorIdx]
            requests.append(update_cells(pivot_sheet_id, heading, 1 + totalColCount, bgColor=bgColor))
            groupColorIdx += 1
        if 'format' in valGroup:
            requests.append(format_cells(pivot_sheet_id, valGroup['format'], valGroup['formatType'],
                startRow=1, numRows=formatNumRows, startCol=1+totalColCount,
                numCols=colCount))
        if 'maxColor' in valGroup:
            percentile = False
            if 'colorPercentile' in valGroup and valGroup['colorPercentile']:
                percentile = True
            requests.append(conditional_format(pivot_sheet_id, maxColor=valGroup['maxColor'], startRow=2,
                numRows=formatNumRows, startCol=1+totalColCount, numCols=colCount, percentile=percentile))
        for val in valGroup['values']:
            totalColCount += 1
            valueList.append({
                'name': val['label'],
                'summarizeFunction': val['summarizeFunction'] if 'summarizeFunction' in val else
                    'sum',
                'formula': val['formula'],
            })
    # Right border at the end
    requests.append(set_column_left_border(pivot_sheet_id, startCol=1+totalColCount,
        numRows=formatNumRows))

    tableDef = {
        'rows': {
            'values': [
                {
                    'pivotTable': {
                        'source': {
                            'sheetId': data_sheet_id,
                            'startRowIndex': 0,
                            'startColumnIndex': 0,
                            'endRowIndex': data_grid_properties['rowCount'],
                            'endColumnIndex': data_grid_properties['columnCount'],
                        },
                        'rows': [
                            table['rows']
                        ],
                        'values': valueList,
                        'valueLayout': 'HORIZONTAL',
                        'filterSpecs': [
                            {
                                'columnOffsetIndex': 1,
                                'filterCriteria': {
                                    'visibleByDefault': True,
                                    'condition': {
                                        'type': 'NOT_BLANK'
                                    }
                                }
                            }
                        ],
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

    requests.append({ 'updateCells': tableDef })

    batch_requests(service, spreadsheet['spreadsheetId'], requests)


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

    if 'schema_tab_map' in config:
        STREAM_MAP = config['schema_tab_map']
        logger.info('Input stream map: {}'.format(STREAM_MAP))

    clear_existing_lines = False
    if 'clear_existing_lines' in config:
        clear_existing_lines = config['clear_existing_lines']


    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    # TODO: initialize empty raw data sheet with column order if it doesn't exist
    state = persist_lines(service, spreadsheet, input, clear_existing_lines)
    spreadsheet = get_spreadsheet(service, config['spreadsheet_id'])
    init_pivot_tables(service, spreadsheet)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
