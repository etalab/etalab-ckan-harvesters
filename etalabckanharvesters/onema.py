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


"""Harvest Onema CSW repository.

http://www.data.eaufrance.fr/
"""


import argparse
import ConfigParser
import logging
import os
import sys

from biryani1 import baseconv, custom_conv, states, strings
from owslib.csw import CatalogueServiceWeb

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, states)
log = logging.getLogger(app_name)


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
        supplier_abbreviation = u'onm',
        supplier_title = u"Office national de l'eau et des milieux aquatiques",
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_site_url = u'http://opendata-sie-back.brgm-rec.fr/geosource/srv/eng/csw'  # Recette environment

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve short infos of packages in source.
    csw = CatalogueServiceWeb(source_site_url)

    bad_indexes = []
    index = 0
    limit = 50
    record_by_id = {}
    while True:
        try:
            csw.getrecords(maxrecords = limit, startposition = index)
        except:
            if limit == 1:
                # Bad record found. Skip it.
                bad_indexes.append(index)
                index += 1
                limit = 50
            else:
                # Retry one by one to find bad record and skip it.
                limit = 1
        else:
            for id, record in csw.records.iteritems():
                record_by_id[id] = record
            next_index = csw.results['nextrecord']
            if next_index <= index:
                break
            index = next_index

    # Retrieve packages from source.
    formats = set()
    groups = [
        harvester.upsert_group(dict(
            title = u'Environnement',
            )),
        ]
    temporals = set()
    types = set()
    for record_id in record_by_id.iterkeys():
        csw.getrecordbyid(id = [record_id])
        record = csw.records[record_id]

        formats.add(record.format)
        temporals.add(record.temporal)
        types.add(record.type)

        if not args.dry_run:
            package = dict(
                license_id = u'fr-lo',
                notes = u'\n\n'.join(
                    fragment
                    for fragment in (
                        record.abstract,
                        record.source,
                        )
                    if fragment
                    ),
                resources = [
                    dict(
                        description = uri.get('description') or None,
                        format = {
                            'CSV': 'CSV',
                            'ESRI Shapefile': 'SHP',
                            'MIF / MID': 'MIF / MID',  # TODO?
                            'RDF': 'RDF',
                            'SHP': 'SHP',
                            'Txt': 'TXT',
                            'WMS': 'WMS',
                            }.get(record.format, record.format),
                        name = uri.get('name'),
                        url = uri['url'],
                        )
                    for uri in record.uris
                    ],
                tags = [
                    dict(name = strings.slugify(subject))
                    for subject in record.subjects
                    ],
#                territorial_coverage = TODO
                # Datasets have a granularity of either "commune" or "poi". Since the both are indexed the same way, use
                # "poi".
                territorial_coverage_granularity = 'poi',
                title = record.title,
#                url = u'URL TODO',
                )

            log.info(u'Harvested package: {}'.format(package['title']))
            harvester.add_package(package, harvester.supplier, record.title, package['url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Formats: {}'.format(sorted(formats)))
    log.info(u'Temporals: {}'.format(sorted(temporals)))
    log.info(u'Types: {}'.format(sorted(types)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
