#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-CKAN-Harvesters -- Harvesters for Etalab's CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Etalab
# http://github.com/etalab/etalab-ckan-harvesters
#
# This file is part of Etalab-CKAN-Harvesters.
#
# Etalab-CKAN-Harvesters is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# Etalab-CKAN-Harvesters is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Download HTML pages from open data repository http://data.ratp.fr/"""


import argparse
import cStringIO
import errno
import logging
import os
import re
import shutil
import sys
import thread
import time
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv
from lxml import etree


app_name = os.path.splitext(os.path.basename(__file__))[0]
args = None
conv = custom_conv(baseconv, datetimeconv)
data_url_path_re = re.compile('/fr/les-donnees/fiche-de-jeu-de-donnees/dataset/(?P<name>[-0-9a-z]+)\.html$')
existing_files_path = set()
html_parser = etree.HTMLParser()
log = logging.getLogger(app_name)
processing_html = False
pool = set()
remaining_html_pages = set()
remaining_links = None
rejected_urls = set()
#search_url_path_re = data_url_path_re
visited_data_names = set()
visited_search_indexes = set()


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('download_dir', help = 'directory where to store downloaded HTML pages')
    parser.add_argument('-c', '--thread-count', default = 1, help = 'max number of threads', type = int)
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    if not os.path.exists(args.download_dir):
        os.makedirs(args.download_dir)

    search_dir = os.path.join(args.download_dir, 'search')
    if os.path.exists(search_dir):
        shutil.rmtree(search_dir)

    existing_dirs = set()
    for (dir, directories_name, filenames) in os.walk(args.download_dir):
        for directory_name in directories_name[:]:
            if directory_name.startswith('.'):
                directories_name.remove(directory_name)
            else:
                existing_dirs.add(os.path.join(dir, directory_name))
        for filename in filenames:
            file_path = os.path.join(dir, filename)
            existing_files_path.add(file_path)

    global remaining_links
    remaining_links = set([
        (u'http://data.ratp.fr/fr/les-donnees.html', 'search'),
        ])
    visited_search_indexes.add(0)

    global processing_html
    processing_html = True
    thread.start_new_thread(process_html_pages, ())
    while remaining_html_pages or processing_html or remaining_links or pool:
        while remaining_links and len(pool) < args.thread_count:
            pool.add(thread.start_new_thread(process_link, remaining_links.pop()))
        time.sleep(0.1)

#    for file_path in existing_files_path:
#        log.info('Removing file %s' % file_path)
#        os.remove(file_path)
    for file_path in existing_files_path:
        log.info('Marking file as deleted: %s' % file_path)
        html_file = open(file_path, 'w')
        html_file.write('deleted')
        html_file.close()

    # Remove obsolete directories.
    # Start with the deeper directories to ensure propagation of deletion to containing directories.
    existing_dirs = list(existing_dirs)
    existing_dirs.sort(reverse = True)
    for dir in existing_dirs:
        if len(os.listdir(dir)) == 0:
            log.info('Removing directory %s' % dir)
            os.rmdir(dir)

    return 0


def process_html_pages():
    global processing_html
    try:
        while remaining_html_pages or remaining_links or pool:
            if not remaining_html_pages:
                time.sleep(0.1)
                continue
            url, html = remaining_html_pages.pop()
            html_doc = etree.parse(cStringIO.StringIO(html), html_parser)
            html_base_list = html_doc.xpath('head/base[@href]')
            base_url = urlparse.urljoin(url, html_base_list[0].get('href')) if html_base_list else url

            # Find URLs of data pages.
            for html_a in html_doc.xpath('//a[@class="detail_link"][@href]'):
                a_url = urlparse.urljoin(base_url, html_a.get('href'))
                if a_url in rejected_urls:
                    continue
                split_url = urlparse.urlsplit(a_url)
                match = data_url_path_re.match(split_url.path)
                assert match is not None, 'Unexpected URL path for data: {0}'.format(a_url)
                name = match.group('name')
                if name in visited_data_names:
                    continue
                visited_data_names.add(name)
                remaining_links.add((a_url, 'data'))

            # Find URLs of search pages.
            for html_a in html_doc.xpath('//ul[@class="tx-pagebrowse"]//li/a[@href]'):
                a_url = urlparse.urljoin(base_url, html_a.get('href'))
                if a_url in rejected_urls:
                    continue
                split_url = urlparse.urlsplit(a_url)
#                match = search_url_path_re.match(split_url.path)
#                assert match is not None, 'Unexpected URL path for search: {0}'.format(a_url)
                url_query = urlparse.parse_qs(split_url.query)
                index = int(url_query['tx_icsoddatastore_pi1[page]'][0]) if url_query else 0
                if index in visited_search_indexes:
                    continue
                visited_search_indexes.add(index)
                remaining_links.add((a_url, 'search'))
    except:
        log.exception(u'An exception occurred in process_html_pages')
    finally:
        processing_html = False


def process_link(url, page_type):
    try:
        split_url = urlparse.urlsplit(url)
        if page_type == 'data':
            match = data_url_path_re.match(split_url.path)
            assert match is not None, 'Unexpected data URL path: {0}'.format(url)
            name = match.group('name')
            html_file_path = os.path.join(args.download_dir, 'data', '{0}.html'.format(name))
        else:
            assert page_type == 'search'
#            match = search_url_path_re.match(split_url.path)
#            assert match is not None, 'Unexpected search URL path: {0}'.format(url)
            url_query = urlparse.parse_qs(split_url.query)
            index = int(url_query['tx_icsoddatastore_pi1[page]'][0]) if url_query else 0
            html_file_path = os.path.join(args.download_dir, 'search', 'search-{0}.html'.format(index))
        log.info('Downloading {0}'.format(url))
        try:
            response = urllib2.urlopen(url.encode('utf-8'))
        except urllib2.HTTPError, error:
            if error.code == 404:
                log.warning('Missing {0}'.format(url))
                rejected_urls.add(url)
                return
            else:
                raise
        html = response.read()
        response.close()

        html_file_dir = os.path.dirname(html_file_path)
        if not os.path.exists(html_file_dir):
            try:
                os.makedirs(html_file_dir)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
        if os.path.exists(html_file_path):
            html_file = open(html_file_path)
            old_html = html_file.read()
            html_file.close()
        else:
            old_html = None
        if html != old_html:
            html_file = open(html_file_path, 'w')
            html_file.write(html)
            html_file.close()
        existing_files_path.discard(html_file_path)
        if page_type == 'search':
            remaining_html_pages.add((url, html))
    except:
        log.exception(u'An exception occurred for {0}'.format(url))
    finally:
        pool.discard(thread.get_ident())


if __name__ == "__main__":
    sys.exit(main())
