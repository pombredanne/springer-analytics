from disco.util import kvgroup
from disco.error import CommError
from disco.core import result_iterator

import re
import datetime
import random

from util import Job, map_with_errors, reduce_with_errors, print_errors
import metadata
import db
import query
import data

download_pattern = re.compile("{ _id: ObjectId\('([^']*)'\), d: ([^,]*), doi: \"([^\"]*)\", i: \"([^\"]*)\", s: ([^,]*), ip: \"([^\"]*)\" }")

class ParseDownloads(Job):
    @staticmethod
    def map(line, params):
        match = jobs.download_pattern.match(line)
        if match:
            (id, date, doi, _, _, ip) = match.groups()
            download = {
                'id':id.decode('latin1').encode('utf8'), 
                'doi':doi.decode('latin1').encode('utf8'), 
                'date':datetime.date(int(date[0:4]), int(date[4:6]), int(date[6:8])), 
                'ip':ip.decode('latin1').encode('utf8')
                } 
            yield id, download
        else:
            yield 'error', line

class FindDataRange(Job):
    partitions = 1

    @staticmethod
    @map_with_errors
    def map((id, download), params):
        yield download['date'], None

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        date, _ = iter.next()
        min_date = date
        max_date = date
        for date, _ in iter:
            min_date = min(min_date, date)
            max_date = max(max_date, date)
        yield 'min_date', min_date
        yield 'max_date', max_date

class PullMetadata(Job):
    sort = True

    partitions = 1

    @staticmethod
    @map_with_errors
    def map((id, download), params):
        yield download['doi'], None

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        for doi, nones in kvgroup(iter):
            try:
                yield doi, metadata.get(doi)
            except db.NotFound:
                try:
                    yield doi, metadata.fetch(doi)
                except CommError, exc:
                    yield 'error', str(exc) # CommError has useless repr
                except Exception, exc:
                    yield 'error', repr(exc)

class BuildHistograms(Job):
    sort = True
    
    @staticmethod
    @map_with_errors
    def map((id, download), params):
        doi = download['doi']
        date = download['date']
        yield doi, date

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        for doi, dates in kvgroup(iter):
            yield doi, data.Histogram(dates, params['min_date'], params['max_date'])

class InvertFeatures(Job):
    sort = True
    
    @staticmethod
    @map_with_errors
    def map((doi, meta), params):
        for feature in metadata.features(doi, meta):
            yield feature, doi

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        for feature, dois in kvgroup(iter):
            yield feature, list(dois)

class TopDownloads(Job):
    @staticmethod
    @map_with_errors
    def map((feature, dois), params):
        totals = [(db.get('histograms', doi).total(), doi) for doi in dois]
        totals.sort()
        top5 = [doi for (total, doi) in totals[-5:]]
        yield feature, top5

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        return iter

class PrecalculateSummaries(Job):
    sort = True
    
    @staticmethod
    @map_with_errors
    def map((feature, dois), params):
        yield feature, None

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        for feature, nones in kvgroup(iter):
            try:
                yield feature, query.evaluate([feature]).next()
            except Exception, exc:
                yield 'error', 'feature %s: %s' % (feature, exc)
 
def build(dump='dump:downloads'):
    downloads = ParseDownloads().run(input=[dump])
    print_errors(downloads)

    find_data_range = FindDataRange().run(input=[downloads.wait()])
    print_errors(find_data_range)
    data_range = dict(result_iterator(find_data_range.results()))
    print data_range
    find_data_range.purge()

    histograms = BuildHistograms().run(input=[downloads.wait()], params=data_range)
    print_errors(histograms)

    db.create('histograms', histograms.wait())
    histograms.purge()

    metadata = PullMetadata().run(input=[downloads.wait()])
    print_errors(metadata)
    downloads.purge()

    db.create('metadata', metadata.wait())

    features = InvertFeatures().run(input=[metadata.wait()])
    print_errors(features)
    metadata.purge()

    db.create('features', features.wait())

    top = TopDownloads().run(input=[features.wait()])
    print_errors(top)
    
    db.create('top-new', top.wait())
    top.purge()

    summaries = PrecalculateSummaries().run(input=[features.wait()])
    print_errors(summaries)
    features.purge()

    db.create('summaries-new', summaries.wait())
    summaries.purge()
    
