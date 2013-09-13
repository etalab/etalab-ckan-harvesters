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


def after_ckan_json_to_organization(organization, state = None):
    if organization is None:
        return None, None
    organization = organization.copy()

    packages = [
        package
        for package in (organization.get('packages') or [])
        if package.get('type') != 'harvest'
        ]
    if not packages:
        return None, None
    organization['packages'] = packages

    if organization.get('private', False) or organization.get('capacity') == u'private':
        return None, None

    return organization, None


def after_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None

#    package = package.copy()

    return package, None


def before_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None

    if package.get('type') == 'harvest':
        return None, None

#    package = package.copy()

    return package, None


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
        supplier_abbreviation = u'onm',
        supplier_title = u"Office national de l'eau et des milieux aquatiques",
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_site_url = u'http://opendata-sie-back.brgm-rec.fr/geosource/srv/eng/csw'  # Recette environment

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
    for record_id in record_by_id.iterkeys():
        csw.getrecordbyid(id = [record_id])
        record = csw.records[record_id]

        package = dict(
            license_id = u'fr-lo',
            notes = record.abstract,
            resources = [
                dict(
                    description = uri.get('description') or None,
                    format = record.format,
                    url = uri['url'],
                    )
                for uri in record.uris
                ],
            tags = [
                dict(name = strings.slugify(subject))
                for subject in record.subjects
                ],
#            territorial_coverage = TODO
            title = record.title,
            )
        source_url = u'URL TODO'
        helpers.set_extra(package, u'Source', source_url)

        log.info(u'Harvested package: {}'.format(package['title']))
        harvester.add_package(package, harvester.supplier, record.title, source_url)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
