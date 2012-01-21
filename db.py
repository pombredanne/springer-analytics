from disco.core import classic_iterator
from disco.worker.classic.func import chain_reader, nop_map
from disco.util import kvgroup
from discodb import DiscoDB

import os
import os.path
import shutil
import random
import datetime
import pickle

from util import Job, map_with_errors, reduce_with_errors
import util

dbs = {}

def dirname(name):
    return os.path.join('/usr/local/var/springer-analytics', name)

def bckname(name):
    return dirname(name + '.' + datetime.datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + '.bck')

def filename(name, partition):
    return os.path.join(dirname(name), str(partition))

class CreateDB(Job):
    map_reader = staticmethod(chain_reader)

    sort = True

    @staticmethod
    @map_with_errors
    def map((key, value), params):
        yield key, pickle.dumps(value)

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        partitions = params['partitions']
        name = params['name']
        discodb = DiscoDB(kvgroup(iter))
        try:
            # figure out what partition we are in
            key = discodb.keys().__iter__().next()
            partition = util.default_partition(key, partitions, params)
            discodb.dump(open(filename(name, partition), 'w'))
            yield partition, None
        except StopIteration:
            # no keys, nothing to write
            pass

partition_size = 1 * 1024 * 1024

def create(name, input):
    # move the existing dir to a backup dir
    dir = dirname(name)
    bck = bckname(name)
    if os.path.exists(dir):
        shutil.move(dir, bck)
    os.makedirs(dir)

    input_size = sum([util.result_size(url) for url in input])
    partitions = 1 + (input_size / partition_size) # close enough
    with open(os.path.join(dir, 'partitions'), 'w') as file:
        file.write(str(partitions))
    job = CreateDB().run(
        input = input,
        partitions = partitions,
        params = {'name':name, 'partitions':partitions}
        )
    created = [key for key, value in classic_iterator(job.wait())]

    load(name)

    # successful - purge job and delete the backup dir
    job.purge()
    if os.path.exists(bck):
        shutil.rmtree(bck)

    return created

def load(name):
    dir = dirname(name)
    with open(os.path.join(dir, 'partitions')) as file:
        partitions = int(file.read())
    discodbs = [DiscoDB()] * partitions
    for partition in xrange(0,partitions):
        path = filename(name, partition)
        if os.path.exists(path):
            discodbs[partition] = DiscoDB.load(open(path))
    dbs[name] = discodbs

def ensure(name):
    if not dbs.has_key(name):
        load(name)
        
def get(name, key):
    ensure(name)
    discodbs = dbs[name]
    partition = util.default_partition(key, len(discodbs), None)
    results = discodbs[partition].get(key)
    if results == None:
        raise NotFound('db:' + name, key)
    else:
        results = list(results)
        if len(results) == 1:
            return util.encode(pickle.loads(results[0]))
        else:
            raise MultipleValues(name, key, results)

def items(name):
    ensure(name)
    for discodb in dbs[name]:
        for key, value in discodb.items():
            yield key, value

class NotFound(Exception):
    def __init__(self, source, key):
        self.source = source
        self.key = key

    def __str__(self):
        return 'NotFound(%s, %s)' % (self.source, self.key)

    def __repr__(self):
        return 'NotFound(%s, %s)' % (self.source, self.key)

class MultipleValues(Exception):
    def __init__(self, name, key, values):
        self.name = name
        self.key = key
        self.values = values

    def __str__(self):
        return 'MultipleValues(%s, %s, %s)' % (self.name, self.key, self.values)

    def __repr__(self):
        return 'MultipleValues(%s, %s, %s)' % (self.name, self.key, self.values)
