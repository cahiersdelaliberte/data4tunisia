# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from udata.models import Resource

from udata.harvest.backends.base import BaseBackend

from bs4 import BeautifulSoup


class BctBackend(BaseBackend):
    name = 'bct'
    display_name = 'bct.gov.tn'

    def initialize(self):
        # TODO: Add base usrl to database
        self.base_url = self.source.url
        # We start at first page
        current_page = 1
        # The first page is statistiques (/stat_index.jsp)
        next_link = '%s/bct/siteprod' % self.base_url
        # Initializing the themes variable
        themes = []

        # While we have a "next_link" to check
        while next_link:
            print 'Requesting page %s' % current_page
            # We get the content of next_link (url)
            main_request = requests.get(next_link)
            # We reinitialize the next_link to False
            # meaning that we may be on the last page
            # (we'll check this later)
            next_link = False
            # Handle case if page does not exist
            if main_request.status_code != 200:
                raise Exception('Could not get main page')
            # We parse the HTML code of the page
            bs = BeautifulSoup(main_request.content, 'html.parser')

            # TODO: Check if BCT statistics website contains pagination.

            # We find all the indexed statistics in the page
            # The table has no id nor class: we grab it using h1.
            h1 = bs.find('h1')
            index_table = h1.find_next('table')

            # re.compile(r"bct-mod-content")
            tbodys = index_table.find_all('tbody', {"class": "bct-mod-content"})
            for tbody in tbodys:
                link = tbody.find_next('td').find('a')
                # For each statistic we get a name
                # and an url
                themes.append({
                    "name": link.getText().strip(),
                    "url": '%s%s' % (self.base_url, link.attrs['href'])
                })

            current_page += 1

        # We went through all the statistics index, now
        # we're going to get datasets from
        # every statistic page #TODO: update bct.py from here
        current_theme_index = 0
        for theme in themes:
            current_theme_index += 1
            print "Now parsing theme \"%s\", %s / %s" % (theme['name'], current_theme_index, len(themes))
            # Let's go to the theme url
            theme_url = theme['url']
            theme_request = requests.get(theme_url)
            # Then parse the theme page HTML code
            bs = BeautifulSoup(theme_request.content, 'html.parser')
            # We find all the datasets links
            datasets_links = bs.find_all('li', {"class": "fils"})
            dataset_index = 0
            # Iterating over the links
            for dataset_link in datasets_links:
                dataset_index += 1
                dataset_link = dataset_link.find('a')
                dataset_name = dataset_link.getText().strip()
                dataset_id = dataset_link.attrs['id']
                try:
                    # Dataset id must be an integer
                    dataset_id = int(dataset_id)
                except:
                    pass
                else:
                    print "Gettings dataset %s / %s for theme \"%s\"" % (dataset_index, len(datasets_links), theme['name'])
                    # If everything is good, we add this dataset
                    # to the list of needed datasets via the add_item method
                    self.add_item(dataset_id, dataset_name=dataset_name, theme_name=theme['name'])

    def process(self, item):
        # dataset_id is found in item.remote_id
        dataset_id = item.remote_id
        # we want to avoid a same id from another
        # source so we add 'instn' in front of it
        dataset = self.get_dataset('instn-%s' % item.remote_id)
        # We saved the theme name as a tag and the dataset name
        # as the title
        dataset.title = item.kwargs['dataset_name']
        dataset.tags = [item.kwargs['theme_name']]
        # We empty the existing resources (= the different "files" for this dataset)
        # Here we will have only one probably
        dataset.resources = []
        # Getting the xls file url
        dataset_request = requests.get('http://beta.ins.tn/fr/node/get/nojs/%s' % dataset_id)
        dataset_soup = BeautifulSoup(dataset_request.content, 'html.parser')
        dataset_xls = dataset_soup.find_all('li', {"class": "data"})
        for xls in dataset_xls:
            url = xls.find('a').attrs['href']
            # We have the url, let's do a HEAD request
            # to get the file size without downloading the whole file
            size_request = requests.head(url)
            file_size = size_request.headers['Content-length']
            # We're good, let's add the file
            # to the dataset resources
            dataset.resources.append(Resource(
                title=dataset.title,
                url=url,
                filetype='remote',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                format='xlsx',
                filesize=file_size
            ))
        # at the end, always return the dataset
        return dataset
