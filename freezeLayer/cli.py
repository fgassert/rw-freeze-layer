#!/usr/bin/env python
from __future__ import unicode_literals

from freezeLayer import *
import requests

try: input = raw_input
except: pass

def ask(msg, validator=None, *args):
    value = input(msg + "\n> ")
    if validator:
        value = validator(value, *args)
    if value:
        return value
    elif value is None:
        return None
    else:
        return ask(msg, validator, *args)

def askYn(msg, defaultY=True):
    default = '(Y/n)' if defaultY else '(y/N)'
    yn = input('{} {} '.format(msg, default)).lower()
    if yn == '':
        return defaultY
    elif yn in ('y', 'yes'):
        return True
    elif yn in ('n', 'no'):
        return False
    else:
        return askYn(msg, defaultY)

def validateLayer(Id):
    if Id == '': return None
    try:
        lyr = rw_api.Layer(Id).get()
        print("Found layer: {}".format(lyr.name))
        if not lyr.provider == "cartodb":
            print('Layer must be of type cartodb')
            return None
        return lyr
    except requests.HTTPError as e:
        print('Could not find layer with Id {}'.format(Id))
        print('({})'.format(','.join(e.args)))
        return False

def validateDateField(field, table):
    try:
        asUTC(getFieldAsList(field, table, limit=1))
    except ValueError as e:
        print('Field {} does not appear to contain valid datetime')
        print('({})'.format(','.join(e.args)))
        return False
    except requests.HTTPError as e:
        print('Invalid field')
        print('({})'.format(','.join(e.args)))
        return False

def validateDate(datestr):
    if datestr == '': return False
    if datestr in ('today', 'yesterday'):
        date = datetime.datetime.combine(datetime.date.today(),
                                         datetime.time())
        if datestr == 'yesterday':
            date -= datetime.timedelta(days=1)
        return asUTC(date)
    try:
        return asUTC(datestr)
    except ValueError as e:
        print('Invalid date.')
        return False

def validateEndDate(datestr, table, time_field):
    date = validateDate(datestr)
    if not date:
        return False
    try:
        checkFutureData(date, table, time_field)
        return date
    except FutureDataError as e:
        print(e)
        if askYn('Continue anyway?', False):
            return date
        return False

def main():
    csql.init()
    if not askYn('\nUse test enviornment ({})?'.format(rw_api.API_URL)):
        rw_api.init(production=True)
        print('Using production: ' + rw_api.util._api_url)
    lyr = ask('\nID of Layer to freeze: ', validateLayer)
    if not lyr: return

    dataset = lyr.getDataset()
    table = dataset.tableName

    if not dataset.mainDateField:
        print('\nDataset does not have mainDateField defined.')
        dataset.mainTimeField = ask('Time field: ', validateDateField, table)
        if dataset.mainDateField:
            if askYn('\nSave {} mainDateField as {} on API?'.format(dataset.name, dataset.mainTimeField), False):
                print('Saving...')
                dataset.push()
        else:
            return
    time_field = dataset.mainDateField

    print ('\nEnter start date for freeze')
    start = ask('(YYYY-MM-DD | today | yesterday): ', validateDate)
    print ('Query start: ' + start.ctime())
    print ('\nEnter end date for freeze')
    end = ask('(YYYY-MM-DD | today | yesterday): ', validateEndDate,
              table, time_field)
    print ('Query end: ' + end.ctime())

    lyr, table = freezeLayer(lyr.Id, start, end, time_field, table, ignore_future=True)

    print ('\nCreated new layer.')
    print ('Layer Id: ' + lyr.Id)
    print ('Layer name: ' + lyr.name)
    print ('http://resourcewatch.org/admin/data/layers/'+lyr.Id)
    print ('\nCreated new table.')
    print ('Table name: "{}".{}'.format(csql.CARTO_USER, table))

    if not askYn('\nKeep new layer and table?'):
        lyr.delete()
        print('Deleted layer: {}'.format(lyr.Id))
        csql.dropTable(table)
        print('Dropped table: {} '.format(table))

    elif askYn('\nRename layer?'):
        lyr.name = ask('Enter name: ', lambda x:x)
        lyr.push()
        print('Renamed layer to ' + lyr.name)

if __name__ == "__main__":
    main()
