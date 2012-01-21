from flask import Flask, render_template, request
import urllib
import time
import datetime

import query
import db
import util

app = Flask('Springer Analytics')

app.template_filter('quote')(urllib.quote)

@app.route('/')
def root():
    return render_template('examples.html')

def format_date(date):
    seconds = time.mktime(date.timetuple())
    return int(seconds * 1000)

def format_summaries(summaries):
    # flot wants key-value pairs instead of a dict
    for summary in summaries:
        for key in summary:
            if key in ['min', '25%', '50%', '75%', 'max', 'mean']:
                summary[key] = sorted(summary[key].items())

@app.route('/search')
def search():
    try:
        summaries, top_downloads = list(query.fetch(query.parse(request.args['query'])))
        format_summaries(summaries)
        return render_template('results.html', summaries=summaries, top_downloads=top_downloads)
    except query.ParseException, exc:
        return render_template('error.html', message=repr(exc))
    except db.NotFound, exc:
        return render_template('error.html', message=repr(exc))

if __name__ == '__main__':
    app.debug = True
    app.run(port=8000)
