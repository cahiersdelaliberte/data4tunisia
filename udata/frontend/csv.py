# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import itertools
import StringIO
import unicodecsv

from datetime import datetime

from flask import Response

from udata.models import db
from udata.core.metrics import Metric


_adapters = {}

CONFIG = {
    'encoding': 'utf-8',
    'delimiter': b';',
    'quotechar': b'"',
}


class Adapter(object):
    '''A Base model CSV adapter'''
    fields = None

    def __init__(self, queryset):
        self.queryset = queryset
        self._fields = None

    def get_fields(self):
        if not self._fields:
            if not isinstance(self.fields, (list, tuple)):
                raise ValueError('Unsupported fields format')
            self._fields = [
                (field, self.getter(field)) if isinstance(field, basestring) else (field[0], self.getter(*field))
                for field in itertools.chain(self.fields, self.dynamic_fields())
            ]
        return self._fields

    def getter(self, name, getter=None):
        if not getter:
            method = 'field_{0}'.format(name)
            return getattr(self, method) if hasattr(self, method) else lambda o: getattr(o, name)
        return (lambda o: getattr(o, getter)) if isinstance(getter, basestring) else getter

    def header(self):
        '''Generate the CSV header row'''
        return [name for name, getter in self.get_fields()]

    def rows(self):
        '''Iterate over queryset objects'''
        return (self.to_row(o) for o in self.queryset)

    def to_row(self, obj):
        '''Convert an object into a flat csv row'''
        row = []
        for name, getter in self.get_fields():
            row.append(getter(obj))
        return row

    def dynamic_fields(self):
        return []


def adapter(model):
    '''Register an adapter class for a given model'''
    def inner(cls):
        _adapters[model] = cls
        return cls
    return inner


def get_adapter(cls):
    return _adapters.get(cls)


def _metric_getter(key, spec):
    return lambda o: o.metrics.get(key, spec.default)


def metric_fields(cls):
    return [
        ('metric.{0}'.format(key), _metric_getter(key, spec))
        for key, spec in Metric.get_for(cls).items()
    ]


def get_writer(out):
    '''Get a preconfigured CSV writer for a given output file'''
    return unicodecsv.writer(out, quoting=unicodecsv.QUOTE_NONNUMERIC, **CONFIG)


def get_reader(infile):
    '''Get a preconfigured CSV reader for a given input file'''
    return unicodecsv.reader(infile, **CONFIG)


def yield_rows(adapter):
    '''Yield a dataset catalog line by line'''
    csvfile = StringIO.StringIO()
    writer = get_writer(csvfile)
    # Generate header
    writer.writerow(adapter.header())
    yield csvfile.getvalue()

    for row in adapter.rows():
        csvfile = StringIO.StringIO()
        writer = get_writer(csvfile)
        writer.writerow(row)
        yield csvfile.getvalue()


def stream(queryset_or_adapter, basename=None):
    '''Stream a csv file from an object list, a queryset or an instanciated adapter'''
    if isinstance(queryset_or_adapter, Adapter):
        adapter = queryset_or_adapter
    elif isinstance(queryset_or_adapter, (list, tuple)):
        if not queryset_or_adapter:
            raise ValueError('Type detection is not possible with an empty list')
        cls = _adapters.get(queryset_or_adapter[0].__class__)
        adapter = cls(queryset_or_adapter)
    elif isinstance(queryset_or_adapter, db.BaseQuerySet):
        cls = _adapters.get(queryset_or_adapter._document)
        adapter = cls(queryset_or_adapter)
    else:
        raise ValueError('Unsupported object type')

    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
    headers = {
        b'Content-Disposition': 'attachment; filename={0}-{1}.csv'.format(basename or 'export', timestamp),
    }
    return Response(yield_rows(adapter), mimetype="text/csv", headers=headers)