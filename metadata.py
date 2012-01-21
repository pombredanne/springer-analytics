import httplib, urllib
import json
import datetime
import time
import random

from discodb import DiscoDB
import os
from disco.error import CommError

import db
import util

keys = json.load(open('keys'))

def fetch(doi, delay=0.3):
    before = time.time()

    # conn = httplib.HTTPConnection('api.springer.com')
    conn = httplib.HTTPConnection('springer.api.mashery.com')
    path = '/metadata/json?%s' % urllib.urlencode({'q':'doi:'+doi, 'api_key':keys['metadata']})
    conn.request('GET', path)
    response = conn.getresponse()
    status = response.status
    data = response.read()
    conn.close()

    after = time.time()
    time.sleep(max(0, delay + before - after))

    if status == 200:
        meta = util.encode(json.loads(data)) # !!! metadata encoding?
        if meta['records']:
            return meta
        else:
            # sometimes get an empty response rather than a 404
            raise db.NotFound('fetch:empty', doi)
    elif status == 404:
        raise db.NotFound('fetch:404', doi)
    else:
        raise CommError(data, 'http://api.springer.com' + path, code=status)

# for testing
def fake(doi):
    def string():
        return random.choice('qwertyuiop')
    def keyed(key, gen):
        return {key: gen()}
    def value(gen):
        return {'value':gen(), 'count':'1'}
    def some(gen, args):
        return [gen(*args) for i in xrange(0,int(random.expovariate(0.5)))]
    def date():
        year = 2010
        month = random.randint(10,12)
        day = random.randint(1,28)
        return '%04d-%02d-%02d' % (year, month, day)
    return {
        'records':[{
                'identifier':'doi:%s' % doi,
                'title': string(),
                'publicationDate': date(),
                'creators': some(keyed, ['creator', string]),
                'publicationName': string(),
                'issn': string(),
                }],
        'facets':[{
                'name':'subject',
                'values':some(value, [string])
                }]
        }

# raises db.NotFound
def get(doi):
    return db.get('metadata', doi)

def features(doi, meta):
    if not meta['records'][0].has_key('doi'):
        yield 'doi:%s' % doi
    for key, value in meta['records'][0].items():
        if (type(value) is str):
            yield '%s:%s' % (key, value)
        elif type(value) is list:
            for subvalue in value:
                yield '%s:%s' % (key, subvalue.values()[0])
    for facet in meta['facets']:
        key = facet['name']
        for value in facet['values']:
            value = value['value']
            yield '%s:%s' % (key, value)

#raises db.NotFound
def publication_date(doi):
    return util.date(get(doi)['records'][0]['publicationDate'])

# raises db.NotFound
def publication(doi):
    record = get(doi)['records'][0]
    if record.has_key('isbn'):
        return 'isbn:' + record['isbn']
    elif record.has_key('issn'):
        return 'issn:' + record['issn']
    else:
        raise NotFound('publication', doi)
