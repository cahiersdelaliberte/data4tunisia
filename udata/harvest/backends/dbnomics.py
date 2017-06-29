# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from udata.models import Resource

from . import BaseBackend, register

import requests
import json


@register
class DbnomicsBackend(BaseBackend):
    # base url should be 'http://widukind-api.cepremap.org/'
    name = 'dbnomics'
    display_name = 'DB.Nomics (Widukind)'
    DBNOMICS_PROVIDERS = [
        'worldbank',
        'imf',
        # 'oecd',
        'bis',
        # 'esri',
        'insee',
        'eurostat',
        # 'ecb',
        # 'bea',
        # 'fed'
    ]

    def initialize(self):
        # Base URL is 'http://widukind-api.cepremap.org/' and
        # has been added to database when creating harvester
        BASE_URL = self.source.url
        for provider in self.DBNOMICS_PROVIDERS:
            # Let's get provider's datasets
            datasets_request = requests.get('%s/api/v1/json/providers/%s/datasets/keys' % (BASE_URL, provider))
            datasets = json.loads(datasets_request.content)['data']
            for dataset in datasets:
                # Let's get dataset filters (codelist) to see if we
                # can filter Tunisia only
                dataset_details = requests.get('%s/api/v1/json/datasets/%s/codelists' % (BASE_URL, dataset))
                codelist_data = json.loads(dataset_details.content)['data']
                filtering_keys = codelist_data.keys()
                found_filtering_key = None
                found_filtering_value = None
                for key in filtering_keys:
                    possible_values = codelist_data[key].keys()
                    for possible_value in possible_values:
                        # Matching value for Tunisia could be
                        # "Tunisia", "Africa: Tunisia", etc
                        if 'Tunisia' in codelist_data[key][possible_value]:
                            found_filtering_key = key
                            found_filtering_value = possible_value
                # We only process the dataset
                # if we found a way to filter Tunisia
                if found_filtering_key and found_filtering_value:
                    self.add_item(dataset, found_filtering_key=found_filtering_key, found_filtering_value=found_filtering_value)

    def process(self, item):
        BASE_URL = self.source.url
        # dataset_slug is found in item.remote_id
        dataset_slug = item.remote_id

        # we want to avoid a same id from another
        # source so we add 'instn' in front of it
        dataset = self.get_dataset('dbnomics-%s' % dataset_slug)
        dataset.title = 'dbnomics-%s' % dataset_slug
        dataset.tags = []

        # We empty the existing resources (= the different "files" for this dataset)
        # Here we will have only one probably
        dataset.resources = []

        found_filtering_key = item.kwargs['found_filtering_key']
        found_filtering_value = item.kwargs['found_filtering_value']
        # We get all the series available for this dataset
        series_request = requests.get('%s/api/v1/json/datasets/%s/series?%s=%s' % (BASE_URL, dataset_slug, found_filtering_key, found_filtering_value))
        series = json.loads(series_request.content)['data']
        for serie in series:
            serie_slug = serie['slug']
            serie_url = "https://db.nomics.world/views/export/series/%s" % serie_slug
            serie_head = requests.head(serie_url)
            csv_size = serie_head.headers['Content-Length']

            dataset.resources.append(Resource(
                title=dataset.title,
                url=serie_url,
                filetype='remote',
                mime='test/csv',
                format='csv',
                filesize=csv_size
            ))
        # at the end, always return the dataset
        return dataset
