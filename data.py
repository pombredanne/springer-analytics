import datetime
import math

import util

class Histogram():
    def __init__(self, items, min_key, max_key):
        # min_key and max_key are inclusive
        self.min_key = min_key
        self.max_key = max_key
        self.counts = {}
        for item in items:
            self.counts[item] = self.counts.get(item, 0) + 1

    def __str__(self):
        return str(dict([(k, self[k]) for k in self]))

    def __repr__(self):
        return repr(dict([(k, self[k]) for k in self]))

    def __contains__(self, item):
        if item in self.counts:
            return True
        elif self.min_key <= item <= self.max_key:
            return True
        else:
            return False

    def __getitem__(self, item):
        if item in self.counts:
            return self.counts[item]
        elif self.min_key <= item <= self.max_key:
            return 0
        else:
            raise KeyError(item)

    def __iter__(self):
        if type(self.min_key) is int:
            return iter(xrange(self.min_key, self.max_key+1))
        elif type(self.min_key) is datetime.date:
            return util.date_range(self.min_key, self.max_key)

    def group_by(self, fun):
        # require that fun is monotonic
        self.min_key = fun(self.min_key)
        self.max_key = fun(self.max_key)
        counts = {}
        for key, count in self.counts.items():
            counts[fun(key)] = counts.get(key, 0) + count
        self.counts = counts

    def total(self):
        return sum(self.counts.values())

class SparseList():
    def __init__(self):
        self.sorted = False
        self.elems = []
        self.num_zeros = 0
        self.num_elems = 0

    def append(self, elem):
        assert(elem >= 0)
        self.num_elems += 1
        if elem == 0:
            self.num_zeros += 1
        else:
            self.elems.append(elem)
            self.sorted = False

    def sort(self):
        if not self.sorted:
            self.elems = sorted(self.elems)
            self.sorted = True

    def __len__(self):
        return self.num_elems

    def __iter__(self):
        for i in range(0, self.num_zeros):
            yield 0
        for elem in self.elems:
            yield elem

    def __getitem__(self, i):
        if i < self.num_zeros:
            return 0
        else:
            return self.elems[i - self.num_zeros]

    def mean(self):
        return sum(self.elems) / float(self.num_elems)

    def min(self):
        if self.num_zeros > 0:
            return 0
        else:
            return min(self.elems)

    def max(self):
        if self.elems:
            return max(self.elems)
        else:
            return 0

    def percentile(self, percentile):
        self.sort()
        index = (self.num_elems - 1) * (percentile / 100.)
        decimal = index % 1
        if decimal == 0:
            return self[int(index)]
        else:
            lower = int(math.floor(index))
            upper = int(math.ceil(index))
            return (1-decimal)*self[lower] + decimal*self[upper]

def summary(histograms):
    keys = set(util.flatten(histograms))
    summary = {
        'elems' : len(histograms),
        'mean' : dict([(k,0.0) for k in keys]),
        'min' : dict([(k,0.0) for k in keys]),
        'max' : dict([(k,0.0) for k in keys]),
        '25%' : dict([(k,0.0) for k in keys]),
        '50%' : dict([(k,0.0) for k in keys]),
        '75%' : dict([(k,0.0) for k in keys]),
        }
    for key in keys:
        values = SparseList()
        for histogram in histograms:
            if key in histogram:
                values.append(histogram[key])
        summary['mean'][key] = float(values.mean())
        summary['min'][key] = float(values.min())
        summary['max'][key] = float(values.max())
        summary['25%'][key] = float(values.percentile(25))
        summary['50%'][key] = float(values.percentile(50))
        summary['75%'][key] = float(values.percentile(75))
    return summary
