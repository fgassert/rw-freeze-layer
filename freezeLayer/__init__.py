#!/usr/bin/env python
"""
rw-freeze-layer

Works only for carto
"""

# Python 2
from __future__ import unicode_literals
try: from builtins import str
except: str = unicode
try: string_types = (str, basestring)
except: string_types = str

import requests
import cartosql as csql
import cartosql.dataset
import os
import logging
import sqlparse
import json
import datetime
import dateutil.parser
import hashlib
import time

try: import rw_api
except: from . import rw_api

def freezeLayer(layerId, start_date, end_date, time_field=None,
                table_name=None, ignore_future=False):
    """
    Copies a CARTO layer's data to new table, and creates an idential layer
    pointing to the new table.

    @params
    layerId      string    the layer to be copied
    start_date   datetime  the start of the period of data to copy
    end_date     datetime  the end of the period of data to copy
    [time_field] string    the field in which datetime information is stored
    [table_name] string    the main table name containing time data
    ignore_future  bool    do not warn if trying to query data in the future

    The table_name and time_field are read from Dataset definition if None.

    @return
    tuple(<rw_api.Layer>, string) the new layer object and tableName
    """

    # 1. Fetch layer and dataset defition
    logging.info('Fetching layer definition for {}'.format(layerId))
    layer = rw_api.getLayer(layerId)
    if not layer.provider == 'cartodb':
        raise("Layer must be of type 'cartodb'")
    if not time_field or not table_name:
        dataset = layer.getDataset()
        time_field = time_field or dataset.mainDateField
        table_name = table_name or dataset.tableName
    sql = layer.layerConfig['options']['sql'].lower()

    # 2. Check if end_date is in future or more recent than the
    # most recent data in the dataset

    # Truncate times to minutes, no need to be too accurate
    start_date = asUTC(start_date).replace(second=0, microsecond=0)
    end_date = asUTC(end_date).replace(second=0, microsecond=0)
    if start_date > end_date:
        start_date, end_date = (end_date, start_date)
    if not ignore_future:
        checkFutureData(end_date, table_name, time_field)

    # 3. Modify the layer SQL query, replacing any where clauses referring to
    # time_field with new ones selecting for the start and end date
    logging.debug('Query: {}'.format(sql))
    start = start_date.isoformat()
    end = end_date.isoformat()
    time_clauses = findTimeClauses(sql, time_field)
    new_cls = " {time_field} >= '{start}' and {time_field} < '{end}'".format(
        time_field=time_field, start=start, end=end)
    for cls in time_clauses:
        sql = sql.replace(cls, new_cls)
    logging.debug('New query: {}'.format(sql))


    # 4. Create the table from the updated layer SQL query

    # Name new table with start and end dates, make sure it isn't too long
    new_table = "{}_{}_{}".format(table_name, start_date.strftime("%Y%m%d_%H%M"), end_date.strftime("%Y%m%d_%H%M"))
    if len(new_table) > 62:
        new_table = "{}_{}".format(new_table[:32], hashlib.md5(new_table[32:]))

    # If we've made this exact query before, replace it
    logging.info("Coping data to table: {}".format(table_name))
    if csql.tableExists(new_table):
        logging.info("Table {} exists, overwriting".format(new_table))
        csql.dropTable(new_table)
    csql.createTableFromQuery(new_table, sql)

    # Wait for table to appear in Carto Viz API?
    # for some reason getting all the datasets seems to help initialize it
    p = None
    while not p:
        try:
            p = csql.dataset.setPrivacy(new_table, 'LINK')
        except Exception as e:
            logging.info('Waiting for dataset to be available...')
            csql.dataset.getDatasets()
            time.sleep(5)

    # 5. Create layer copy and update SQL to refer to new table
    layer_name = "{} ({} to {})".format(layer.name, start, end)
    new_lyr = layer.copy(layer_name)
    new_lyr.layerConfig['options']['sql'] = "SELECT * FROM {}".format(new_table)
    new_lyr.published = False

    logging.info("Uploading new layer {}".format(layer_name))
    new_lyr.push()

    return (new_lyr, new_table)


### Utility functions

# Managing time
class FutureDataError(Exception):
    ''''''
    pass

def asUTC(date, tz=None):
    ''''''
    if isinstance(date, string_types):
        date = dateutil.parser.parse(date, fuzzy=True)
    if date.tzinfo is None:
        if tz is None:
            logging.debug('Assuming time already in utc')
            date.replace(tzinfo=dateutil.tz.UTC)
        else:
            date = date.astimezone(dateutil.tz.UTC)
    return date

def checkFutureData(date, table_name, time_field):
    '''Check if date is more recent than latest data'''
    latest_date = getFieldAsList(time_field, table_name,
                                 order='{} DESC LIMIT 1'.format(time_field))[0]
    latest_date = asUTC(latest_date)
    now = asUTC(datetime.datetime.utcnow())
    logging.debug('Now: ' + now.ctime())
    logging.debug('Latest: ' + latest_date.ctime())
    logging.debug('Query end: ' + date.ctime())
    if date > now:
        warn = "End date is in the future! The frozen dataset will not update to include that data if it is added in the future."
        raise(FutureDataError(warn))
    elif date > latest_date:
        warn = "End date is more recent than the latest data in the table! The frozen dataset will not update to include that data if it is added in the future."
        raise(FutureDataError(warn))
    return date

def getFieldAsList(field, table, **args):
    ''''''
    return csql.getFields(field, table, f='csv', **args).text.splitlines()[1:]

# Parsing SQL
def _findWheres(group):
    '''Find all the where clauses'''
    matches = []
    for t in group.tokens:
        if isinstance(t, sqlparse.sql.Where):
            matches.append(t)
        elif t.is_group:
            matches.extend(_findWheres(t))
    return matches


def _findTimeClause(whereClause, timename):
    '''Search for expressions within whereClause that contain timename'''
    tokens = [t for t in whereClause.flatten()]
    matches = []
    start = None
    for i in range(len(tokens)):
        if tokens[i].match(sqlparse.sql.T.Name, timename):
            # if you find the time field, look backwards till you find
            # the beginning of the expression marked by 'and', 'or', or 'where'
            start = 1
            for j in range(i, 0, -1):
                if tokens[j].match(sqlparse.sql.T.Keyword,
                                   ('and', 'or', 'where')):
                    start = j + 1
        elif start:
            # if you've found the time field, keep looking forwards
            # until you find the end of the expression...
            if tokens[i].match(sqlparse.sql.T.Keyword, ('and', 'or', 'where')):
                cls = sqlparse.sql.TokenList(tokens[start:i-1]).normalized
                matches.append(cls)
                start = None

    # ...or just get to the end of the where clause
    if start:
        matches.append(sqlparse.sql.TokenList(tokens[start:-1]).normalized)
    return matches

def findTimeClauses(sql, timename):
    '''Return a list of all WHERE expressions referring to timename'''
    statement = sqlparse.parse(sql)[0]
    timeexprs = []
    for w in _findWheres(statement):
        timeexprs.extend(_findTimeClause(w, timename))
    return timeexprs

# test
def test():
    ''''''
    logging.basicConfig(level=logging.DEBUG)
    lyr = 'b4f1bd67-d0b7-4b53-a815-1638761ca70f'
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(days=1)
    time_field = 'utc'
    table_name = 'cit_003a_air_quality_pm25'
    layer, table = freezeLayer(lyr, start_time, end_time, time_field, table_name, True)
    print(('Created: ', layer.Id, table))
    layer.delete()
    csql.dropTable(table)
    print(('Deleted: ', layer.Id, table))


if __name__ == "__main__":
    test()
