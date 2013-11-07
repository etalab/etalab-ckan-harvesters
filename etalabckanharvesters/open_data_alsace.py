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


"""Harvest open data from "Open Data Alsace".

http://opendata.alsace.fr/
"""


import argparse
import ConfigParser
import logging
import os
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, jsonconv, states)
frequency_by_updateFrequency = {
    u'Annuelle': u'annuelle',
    u'Journalière': u'quotidienne',
    u'Mensuelle': u'mensuelle',
    }
license_id_by_summary = {
    u"Licence Libre": u'other-free',
    u"Licence ouverte Alsace": u'other-at',
    }
log = logging.getLogger(app_name)
N_ = lambda message: message
region_title_by_owner = {
    'cigal': u'Coopération pour l’Information Géographique en Alsace (CIGAL)',
    'region': u'Région Alsace',
    }


json_to_results = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            results = conv.pipe(
                conv.test_isinstance(list),
                ),
            ),
        ),
    conv.function(lambda value: value['results']),
    )


json_to_single_result = conv.pipe(
    json_to_results,
    conv.test(lambda value: len(value) == 1, error = N_(u'Not a singleton list')),
    conv.function(lambda value: value[0]),
    )


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-d', '--dry-run', action = 'store_true',
        help = "simulate harvesting, don't update CKAN repository")
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
        supplier_abbreviation = u'als',
        supplier_title = u'Open Data Alsace',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://opendata.alsace.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    request = urllib2.Request(urlparse.urljoin(source_site_url, u'dataserver/CRAL/catalog/Source?$format=json'),
        headers = source_headers)
    response = urllib2.urlopen(request)
    table_metadata = conv.check(conv.pipe(
        conv.make_input_to_json(),
        conv.test_isinstance(dict),
        conv.struct(
            dict(
                d = conv.pipe(
                    json_to_results,
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.test_isinstance(dict),
                            conv.not_none,
                            ),
                        ),
                    conv.not_none,
                    ),
                ),
            ),
        ))(response.read(), state = conv.default_state)
    for entry in table_metadata['d']:
        entry = conv.check(conv.struct(
            dict(
                __metadata = conv.pipe(
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            type = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.test_in([
                                    u"fr.mgdis.odata.catalog.SourceType",
                                    ]),
                                conv.not_none,
                                ),
                            uri = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.make_input_to_url(full = True),
                                conv.not_none,
                                ),
                            ),
                        ),
                    conv.not_none,
                    ),
                categories = conv.pipe(
                    json_to_single_result,
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            name = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.cleanup_line,
                                conv.not_none,
                                ),
                            uri = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.translate({
                                    u'none': None,
                                    }),
                                conv.test_none(),
                                ),
                            ),
                        ),
                    conv.function(lambda value: value['name']),
                    conv.not_none,
                    ),
                copyright = conv.pipe(
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            licences = conv.pipe(
                                json_to_single_result,
                                conv.test_isinstance(dict),
                                conv.struct(
                                    dict(
                                        summary = conv.pipe(
                                            json_to_single_result,
                                            conv.test_isinstance(dict),
                                            conv.struct(
                                                dict(
                                                    language = conv.pipe(
                                                        conv.test_isinstance(basestring),
                                                        conv.test_in([
                                                            u"fr-FR",
                                                            ]),
                                                        conv.not_none,
                                                        ),
                                                    text = conv.pipe(
                                                        conv.test_isinstance(basestring),
                                                        conv.test_in(license_id_by_summary),
                                                        conv.not_none,
                                                        ),
                                                    ),
                                                ),
                                            conv.function(lambda value: value['text']),
                                            conv.not_none,
                                            ),
                                        url = conv.pipe(
                                            conv.test_isinstance(basestring),
                                            conv.translate({
                                                u'None': None,
                                                }),
                                            conv.test_none(),
                                            ),
                                        ),
                                    ),
                                conv.function(lambda value: value['summary']),
                                conv.not_none,
                                ),
                            owner = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.test_in(region_title_by_owner),
                                conv.not_none,
                                ),
                            ),
                        ),
                    conv.not_none,
                    ),
                createdOnDate = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.datetime_to_iso8601_str,
                    conv.not_none,
                    ),
                descriptions = conv.pipe(
                    json_to_single_result,
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            locale = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.test_in([
                                    u'fr',
                                    u'fr-FR',
                                    ]),
                                conv.not_none,
                                ),
                            keywords = conv.pipe(
                                json_to_results,
                                conv.uniform_sequence(
                                    conv.input_to_slug,
                                    drop_none_items = True,
                                    ),
                                conv.empty_to_none,
                                ),
                            subtitle = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.cleanup_line,
                                conv.not_none,
                                ),
                            summary = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.cleanup_line,
                                conv.not_none,
                                ),
                            title = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.cleanup_line,
                                conv.not_none,
                                ),
                            thumbnail = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.make_input_to_url(full = True),
                                ),
                            ),
                        ),
                    conv.not_none,
                    ),
                extensions = conv.pipe(
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            __deferred = conv.pipe(
                                conv.test_isinstance(dict),
                                conv.struct(
                                    dict(
                                        uri = conv.pipe(
                                            conv.test_isinstance(basestring),
                                            conv.make_input_to_url(full = True),
                                            conv.not_none,
                                            ),
                                        ),
                                    ),
                                conv.not_none,
                                ),
                            ),
                        ),
                    conv.not_none,
                    ),
                history = conv.pipe(
                    json_to_results,
                    # TODO,
                    conv.not_none,
                    ),
                id = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                lastUpdateDate = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.datetime_to_iso8601_str,
                    conv.not_none,
                    ),
                locale = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.test_in([
                        u"fr",
                        u"fr-FR",
                        ]),
                    conv.not_none,
                    ),
                privacy = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.test_in([
                        u'PUBLIC',
                        ]),
                    conv.not_none,
                    ),
                schema = conv.pipe(
                    conv.test_isinstance(dict),
                    # TODO
                    conv.not_none,
                    ),
                updateFrequency = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.test_in(frequency_by_updateFrequency),
                    conv.not_none,
                    ),
                ),
            ))(entry, state = conv.default_state)

        groups = [
            harvester.upsert_group(dict(
                title = entry[u'categories'],
                )),
            ]

        # TODO: There is currently no way to access a dataset by its own URL.
        html_url = source_site_url

        package = dict(
            frequency = frequency_by_updateFrequency.get(entry[u'updateFrequency']),
            license_id = license_id_by_summary[entry[u'copyright'][u'licences']],
            notes = u'\n\n'.join(
                fragment
                for fragment in (
                    entry[u'descriptions'][u'subtitle'],
                    entry[u'descriptions'][u'summary'],
                    )
                ),
            resources = [
                dict(
                    created = entry['createdOnDate'].split('T')[0],
                    format = u'CSV',
                    last_modified = entry['lastUpdateDate'].split('T')[0],
                    name = u'Fichier au format CSV',
                    url = u'http://opendata.alsace.fr/dataserver/CRAL/data/{}?$format=csv'.format(entry['id']),
                    ),
                dict(
                    created = entry['createdOnDate'].split('T')[0],
                    format = u'JSON',
                    last_modified = entry['lastUpdateDate'].split('T')[0],
                    name = u'Fichier au format CSV',
                    url = u'http://opendata.alsace.fr/dataserver/CRAL/data/{}?$format=json'.format(entry['id']),
                    ),
                dict(
                    created = entry['createdOnDate'].split('T')[0],
                    format = u'XML',
                    last_modified = entry['lastUpdateDate'].split('T')[0],
                    name = u'Fichier au format AtomPub',
                    url = u'http://opendata.alsace.fr/dataserver/CRAL/data/{}'.format(entry['id']),
                    ),
                ],
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(entry['descriptions']['keywords'] or []))
                ],
            territorial_coverage = u'RegionOfFrance/42/Alsace',
            title = entry[u'descriptions'][u'title'],
            url = html_url,
            )

        related = [
            dict(
                # description = None,
                image_url = entry['descriptions']['thumbnail'],
                title = entry['descriptions']['title'],
                type = u'visualization',
                # url = None,
                ),
            ] if entry['descriptions']['thumbnail'] is not None else None

        if not args.dry_run:
            organization = harvester.upsert_organization(dict(
                title = region_title_by_owner[entry[u'copyright'][u'owner']],
                ))

            harvester.add_package(package, organization, entry[u'id'], html_url, groups = groups, related = related)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
