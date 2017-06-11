# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from udata.models import Resource

from . import BaseBackend, register

import requests
from bs4 import BeautifulSoup


@register
class InstnBackend(BaseBackend):
    # base url should be 'http://beta.ins.tn'
    name = 'instn'
    display_name = 'Ins.tn'

    def initialize(self):
        '''Generate a list of fake identifiers to harvest'''
        # In a real implementation, you should iter over
        # a remote endpoint to list identifiers to harvest
        # and optionnaly store extra data
        self.base_url = self.source.url
        current_page = 1
        next_link = '%s/fr/glossaire-satistique' % self.base_url
        themes = []

        while next_link:
            print 'Requesting page %s' % current_page
            main_request = requests.get(next_link)
            next_link = False
            if main_request.status_code != 200:
                raise Exception('Could not get main page')
            bs = BeautifulSoup(main_request.content, 'html.parser')
            page_links = bs.find('ul', {"class": "pager"}).find_all('a')

            h3s = bs.find('div', {"class": "conteneur_block"}).find_all('h3')
            for h3 in h3s:
                link = h3.find('a')
                themes.append({
                    "name": link.getText().strip(),
                    "url": '%s%s' % (self.base_url, link.attrs['href'])
                })

            for page_link in page_links:
                try:
                    page_number = int(page_link.getText())
                except:
                    pass
                else:
                    if page_number == current_page + 1 and False:
                        next_link = '%s%s' % (self.base_url, page_link.attrs['href'])
            current_page += 1

        current_theme_index = 0
        for theme in themes:
            if current_theme_index >= 1:
                continue
            current_theme_index += 1
            print "Now parsing theme \"%s\", %s / %s" % (theme['name'], current_theme_index, len(themes))
            theme_url = theme['url']
            theme_request = requests.get(theme_url)
            bs = BeautifulSoup(theme_request.content, 'html.parser')
            datasets_links = bs.find_all('li', {"class": "fils"})
            dataset_index = 0
            for dataset_link in datasets_links:
                dataset_index += 1
                dataset_link = dataset_link.find('a')
                dataset_name = dataset_link.getText().strip()
                dataset_id = dataset_link.attrs['id']
                try:
                    dataset_id = int(dataset_id)
                except:
                    pass
                else:
                    print "Gettings dataset %s / %s for theme \"%s\"" % (dataset_index, len(datasets_links), theme['name'])
                    self.add_item(dataset_id, dataset_name=dataset_name, theme_name=theme['name'])

    def process(self, item):
        '''Generate a random dataset from a fake identifiers'''
        # Get or create an harvested dataset with this identifier.
        # Harvest metadata are already filled on creation.
        dataset = self.get_dataset(item.remote_id)
        dataset_id = item.remote_id
        dataset.title = item.kwargs['dataset_name']
        dataset.tags = [item.kwargs['theme_name']]
        dataset.resources = []
        dataset_request = requests.get('http://beta.ins.tn/fr/node/get/nojs/%s' % dataset_id)
        dataset_soup = BeautifulSoup(dataset_request.content, 'html.parser')
        dataset_xls = dataset_soup.find_all('li', {"class": "data"})
        for xls in dataset_xls:
            url = xls.find('a').attrs['href']

            size_request = requests.head(url)
            file_size = size_request.headers['Content-length']

            dataset.resources.append(Resource(
                title=dataset.title,
                url=url,
                filetype='remote',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                format='xlsx',
                filesize=file_size
            ))

        return dataset
