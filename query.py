from pyparsing import *
import math

import db
import metadata
import util
import data

p_key = Word(alphanums)
p_value = Word(alphanums + '-' + '/' + '_') ^ QuotedString('"', escChar='\\')
p_pair = Group(p_key + ':' + p_value)
p_query = delimitedList(p_pair) + LineEnd()

# raises pyparsing.ParseException
def parse(string):
    return [''.join(pair) for pair in p_query.parseString(string)]

# raises db.NotFound
def evaluate(query):
    for feature in query:
        histograms = []
        for doi in db.get('features', feature):
            histogram = db.get('histograms', doi)
            pubdate = metadata.publication_date(doi)
            histogram.group_by(lambda (date): (date-pubdate).days / 30)
            histograms.append(histogram)
        summary = data.summary(histograms)
        summary['feature'] = feature
        yield summary

# raises db.NotFound
def fetch(query):
    summaries = [db.get('summaries', feature) for feature in query]
    top_downloads = [db.get('top', feature) for feature in query]
    return (summaries, top_downloads)
    
