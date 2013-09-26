#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-CKAN-Harvesters -- Harvesters for Etalab's CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
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


"""Harvest Data Publica repository.

http://www.data-publica.com/
"""


import argparse
import ConfigParser
import datetime
import itertools
import json
import logging
import os
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, states, strings
from ckantoolbox import ckanconv

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, datetimeconv, states)
format_by_mime_type = {
    u'application/api': u'api',
    u'application/visualisation': u'viz',
    u'application/vnd.ms-excel': u'xls',
    u'application/x-gzip': u'gzip',
    u'application/xhtml+xml': u'html',
    u'application/xml': u'xml',
    u'text/tsv': u'tsv',
    }
license_id_by_name = {
    u'Licence Banque Mondiale': u'fr-lo',
    u'Licence EuroStat': u'fr-lo',
    u'Licence Information Publique': u'fr-lo',
    }
log = logging.getLogger(app_name)
now_str = datetime.datetime.now().isoformat()
today_str = datetime.date.today().isoformat()

validate_publication = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            id = conv.pipe(
                conv.test_isinstance(basestring),
                conv.not_none,
                ),
            groups = conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(basestring),
#                        conv.test_in([
#                            u'Agriculture & Pêche',
#                            u'Economie & Finances',
#                            u'Education & Formation',
#                            u'Environnement & Energie',
#                            u'Immobilier & Construction',
#                            u'Industrie & Production',
#                            u'Population & Démographie',
#                            u'Sociétés & Conditions de vie',
#                            u'Transport & Logistique',
#                            u'Travail & Salaires',
#                            u'Tourisme & Voyages',
#                            ]),
                        conv.not_none,
                        ),
                    ),
                ),
            licenseName = conv.pipe(
                conv.test_isinstance(basestring),
                conv.test_in(license_id_by_name),
                conv.not_none,
                ),
            metadata_created = conv.pipe(
                conv.timestamp_to_datetime,
                conv.datetime_to_iso8601_str,
                conv.not_none,
                ),
            name = conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            notes = conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_text,
                conv.not_none,
                ),
            organization = conv.pipe(
                conv.test_isinstance(basestring),
                conv.not_none,
                ),
            opendata = conv.pipe(
                conv.test_isinstance(bool),
                conv.test_equals(True),
                conv.not_none,
                ),
            resources = conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(dict),
                        conv.struct(
                            dict(
                                mimeType = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.test_in(format_by_mime_type),
                                    conv.not_none,
                                    ),
                                name = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.cleanup_line,
                                    conv.not_none,
                                    ),
                                url = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.make_input_to_url(),  # URL is not full.
                                    conv.not_none,
                                    ),
                                ),
                            ),
                        conv.not_none,
                        ),
                    ),
                ),
            revision_timestamp = conv.pipe(
                conv.timestamp_to_datetime,
                conv.datetime_to_iso8601_str,
                conv.not_none,
                ),
            supplier = conv.pipe(
                conv.test_isinstance(basestring),
                conv.test_equals(u"Data Publica"),
                conv.not_none,
                ),
            tags = conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(dict),
                        conv.struct(
                            dict(
                                name = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.not_none,
                                    ),
                                ),
                            ),
                        conv.not_none,
                        ),
                    ),
                ),
            url = conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(),  # URL is not full.
                conv.not_none,
                ),
            ),
        ),
    )


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(here = os.path.dirname(args.config)))
    config_parser.read(args.config)
    conf = conv.check(conv.pipe(
        conv.test_isinstance(dict),
        conv.struct(
            {
                'ckan.api_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'ckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'user_agent': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                },
            default = 'drop',
            ),
        conv.not_none,
        ))(dict(config_parser.items('Etalab-CKAN-Harvesters')), conv.default_state)

    harvester = helpers.Harvester(
        supplier_abbreviation = u'dp',
        supplier_title = u'Data Publica',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://www.data-publica.com/etalab/export'

    harvester.retrieve_target()

    # Retrieve packages from source.
    for page_index in itertools.count():
        page_url = urlparse.urljoin(source_site_url, u'?p={}'.format(page_index))
        log.info(u"Harvesting page {}".format(page_url))
        request = urllib2.Request(page_url.encode('utf-8'), headers = source_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        publications = response_dict['publications']
        if not publications:
            break
        for publication in publications:
            publication = conv.check(conv.pipe(
                validate_publication,
                conv.not_none,
                ))(publication, state = conv.default_state)

            organization_title = publication['organization']
            if organization_title is None:
                organization = harvester.supplier
            else:
                organization = harvester.upsert_organization(dict(
                    title = organization_title,
                    ))

            package = dict(
                license_id = license_id_by_name[publication['licenseName']],
                notes = publication['notes'],
                resources = [
                    dict(
                        name = resource['name'],
                        format = format_by_mime_type[resource['mimeType']],
                        url = urlparse.urljoin(source_site_url, resource['url']),
                        )
                    for resource in (publication['resources'] or [])
                    ],
                tags = [
                    dict(name = strings.slugify(tag_name))
                    for tag_name in sorted(itertools.chain(
                        (
                            tag['name']
                            for tag in (publication['tags'] or [])
                            ),
                        (
                            sub_fragment
                            for group in (publication['groups'] or [])
                            for fragment in group.split(u'&')
                            for sub_fragment in fragment.split(u',')
                            ),
                        ))
                    ],
#                territorial_coverage = u'Country/FR',
                title = publication['name'],
                )
            source_url = urlparse.urljoin(source_site_url, publication['url'])
            helpers.set_extra(package, u'Source', source_url)

            log.info(u'Harvested package: {}'.format(package['title']))
            harvester.add_package(package, organization, publication['name'], source_url)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
