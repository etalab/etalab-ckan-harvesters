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


"""Harvest open data from "SNCF Open Data".

http://data.sncf.com/
"""


import argparse
import ConfigParser
import logging
import os
import subprocess
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, jsonconv, states)
format_by_mimetype = {
    u'application/octet-stream': None,
    u'application/pdf': u'PDF',
    }
frequency_by_accrualperiodicity = {
    u'Annuel': u"annuelle",
    u'Annuelle': u"annuelle",
    u'Sur événements': u"ponctuelle",
    u'Tous les deux mois': u"bimestrielle",
    u'Toutes les semaines': u"hebdomadaire",
    u'Variable': u"ponctuelle",
    }
granularity_translations = {
    u'Gare': u'poi',
    u'Par gare': u'poi',
    }
license_id_by_license = {
    u'Données Confidentielles': u'other-closed',
    u'SNCF Open Data': u'other-open',
    u"Open Data Commons Open Database License (ODbL)": u'odc-odbl',
    u'Open Database License (ODbL)': u'odc-odbl',
    }
log = logging.getLogger(app_name)
#N_ = lambda message: message
author_by_publisher = {
    u'Intercités': u'Intercités',
    u"OpenStreetMap": None,
    u'SNCF - Transilien': u'SNCF - Transilien',
    u"TER": u"TER",
    u"Transilien": u"Transilien",
    }
territory_by_tag_name = {
    u'france': u'Country/FR/FRANCE',
    u'ile-de-france': u'RegionOfFrance/11/ILE DE FRANCE',
    }


# Converters


def html_to_markdown(value, state = None):
    if value is None:
        return value, None
    process = subprocess.Popen(['pandoc', '-f', 'html', '-t', 'markdown'], stdin = subprocess.PIPE,
        stdout = subprocess.PIPE)
    stdout, stderr = process.communicate(value.encode('utf-8'))
    return stdout.decode('utf-8'), None


json_to_dataset = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            attachments = conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(dict),
                        conv.struct(
                            dict(
                                id = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.empty_to_none,
                                    conv.not_none,
                                    ),
                                mimetype = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.test_in(format_by_mimetype),
                                    conv.not_none,
                                    ),
                                title = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.cleanup_line,
                                    conv.not_none,
                                    ),
                                url = conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.make_input_to_url(full = True, schemes = ('odsfile',)),
                                    conv.test(lambda url: url.startswith(u'odsfile://datasncf/')),
                                    conv.not_none,
                                    ),
                                ),
                            ),
                        conv.not_none,
                        ),
                    ),
                conv.not_none,
                ),
            datasetid = conv.pipe(
                conv.test_isinstance(basestring),
                conv.empty_to_none,
                conv.not_none,
                ),
            fields = conv.noop,
            features = conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(basestring),
                        conv.test_in([
                            u'analyze',
                            u'geo',
                            u'timeserie',
                            ]),
                        conv.not_none,
                        ),
                    ),
                conv.not_none,
                ),
            has_records = conv.pipe(
                conv.test_isinstance(bool),
                conv.not_none,
                ),
            metas = conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    dict(
                        accrualperiodicity = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_in(frequency_by_accrualperiodicity),
                            ),
                        created = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.iso8601_input_to_date,
                            conv.date_to_iso8601_str,
                            ),
                        creator = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.test_in([
                                u'SNCF Transilien',
                                ]),
                            ),
                        description = conv.pipe(
                            conv.test_isinstance(basestring),
                            html_to_markdown,
                            conv.cleanup_text,
                            conv.not_none,
                            ),
                        domain = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_equals(u'datasncf'),
                            conv.not_none,
                            ),
                        granularity = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.test_in(granularity_translations),
                            ),
                        issued = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.iso8601_input_to_date,
                            conv.date_to_iso8601_str,
                            ),
                        keyword = conv.pipe(
                            conv.test_isinstance(list),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.input_to_slug,
                                    conv.not_none,
                                    ),
                                ),
                            conv.not_none,
                            ),
                        language = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.translate({
                                u'7 jours à partir de la date de publication': None,
                                }),
                            conv.test_equals(u'FR'),
                            ),
                        license = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_in(license_id_by_license),
                            conv.not_none,
                            ),
                        modified = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.iso8601_input_to_datetime,
                            conv.datetime_to_iso8601_str,
                            conv.not_none,
                            ),
                        publisher = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_in(author_by_publisher),
                            conv.not_none,
                            ),
                        spatial = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.make_input_to_url(full = True),
                            ),
                        temporal = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.translate({
                                u'N/A': None,
                                }),
                            conv.test_in([
                                u'60 jours à partir de la date de publication',
                                ]),
                            ),
                        theme = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        title = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        visibility = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_in([u'domain', u'public']),
                            conv.not_none,
                            ),
                        ),
                    ),
                conv.not_none,
                ),
            ),
        ),
    )


# Functions


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
        supplier_abbreviation = u'sncf',
        supplier_title = u'Société nationale des chemins de fer français',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://ressources.data.sncf.com/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    datasets = []
    while True:
        request = urllib2.Request(urlparse.urljoin(source_site_url, u'api/datasets/1.0/search/?start={}'.format(
            len(datasets))), headers = source_headers)
        response = urllib2.urlopen(request)
        response_dict = conv.check(conv.pipe(
            conv.make_input_to_json(),
            conv.test_isinstance(dict),
            conv.struct(
                dict(
                    datasets = conv.pipe(
                        conv.test_isinstance(list),
                        conv.uniform_sequence(
                            conv.pipe(
                                conv.test_isinstance(dict),
                                conv.not_none,
                                ),
                            ),
                        conv.not_none,
                        ),
                    ),
                default = conv.noop,
                ),
            conv.not_none,
            ))(response.read(), state = conv.default_state)
        if not response_dict['datasets']:
            break
        datasets.extend(response_dict['datasets'])

    all_tags_name = set()
    for entry in datasets:
        dataset = conv.check(json_to_dataset)(entry, state = conv.default_state)
        metas = dataset['metas']

        tags_name = metas[u'keyword']
        all_tags_name.update(tags_name)

        author = author_by_publisher[metas['publisher']]
        if author is None:
            log.info(u'Ignoring dataset "{}" from "{}"'.format(metas[u'title'], metas[u'publisher']))
            continue

        resources = []
        if dataset[u'has_records']:
            for format in (u'csv', 'json', 'xls'):
                resources.append(dict(
                    created = metas['created'] or metas[u'issued'],
                    format = format.upper(),
                    last_modified = metas['modified'].split()[0],
                    name = u'Fichier {}'.format(format.upper()),
                    url = urlparse.urljoin(source_site_url, u'explore/dataset/{}/download?format={}'.format(
                        dataset[u'datasetid'], format)),
                    ))
            if 'geo' in dataset[u'features']:
                resources.append(dict(
                    created = metas['created'] or metas[u'issued'],
                    format = u'SHP',
                    last_modified = metas['modified'].split()[0],
                    name = u'Fichier SHAPEFILE',
                    url = urlparse.urljoin(source_site_url, u'explore/dataset/{}/download?format=shp'.format(
                        dataset[u'datasetid'])),
                    ))
        for attachment in dataset['attachments']:
            resources.append(dict(
                created = metas['created'] or metas[u'issued'],
                format = format_by_mimetype[attachment[u'mimetype']],
                last_modified = metas['modified'].split()[0],
                name = attachment[u'title'],
                url = urlparse.urljoin(source_site_url,
                    u'api/datasets/1.0/{}/attachments/{}'.format(dataset[u'datasetid'], attachment[u'id'])),
                ))
        if metas['spatial'] is not None:
            resources.append(dict(
                created = metas['created'] or metas[u'issued'],
                format = u'API',
                last_modified = metas['modified'].split()[0],
                name = u'API',
                url = metas['spatial'],
                ))
        package = dict(
            author = author,
            frequency = frequency_by_accrualperiodicity.get(metas[u'accrualperiodicity']),
            license_id = license_id_by_license[metas[u'license']],
            notes = metas[u'description'],
            resources = resources,
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(tags_name)
                if tag_name not in territory_by_tag_name
                ],
            territorial_coverage = u','.join(
                territory
                for territory in (
                    territory_by_tag_name.get(tag_name)
                    for tag_name in sorted(tags_name)
                    )
                if territory is not None
                ),
            territorial_coverage_granularity = granularity_translations.get(metas[u'granularity']),
            title = metas[u'title'],
            url = urlparse.urljoin(source_site_url, u'explore/dataset/{}/'.format(dataset[u'datasetid'])),
            )

        related = None
#        related = [
#            dict(
#                # description = None,
#                image_url = entry['descriptions']['thumbnail'],
#                title = entry['descriptions']['title'],
#                type = u'visualization',
#                # url = None,
#                ),
#            ] if entry['descriptions']['thumbnail'] is not None else None

        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = metas[u'theme'],
                    )),
                ]

            harvester.add_package(package, harvester.supplier, dataset[u'datasetid'], package[u'url'], groups = groups,
                related = related)

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Tags: {}'.format(u', '.join(sorted(all_tags_name))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
