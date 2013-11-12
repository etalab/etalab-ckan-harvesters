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


"""Harvest "Open Data Hauts-de-Seine".

* Front Site: http://opendata.hauts-de-seine.net/
* CKAN Site: http://api-opendata.hauts-de-seine.net/
"""


import argparse
import ConfigParser
import datetime
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states, strings
from ckantoolbox import ckanconv

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, datetimeconv, jsonconv, states)
license_id_translations = {
    u'En cours de détermination': u'other-open',
    u'licence ouverte': u'fr-lo',
    }
type_translations = {
    u'Jeu de données (Série de données)': u'dataset',
    }
log = logging.getLogger(app_name)
source_site_url = None


def after_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None

    if package.get('private', False) or package.get('capacity') == u'private':
        return None, None

    return package, None


def before_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
    package = package.copy()

    groups = package.get('groups')
    if groups is not None:
        package['groups'] = []
        for group in groups:
            group = group.copy()
            image_url = group.get('image_url')
            if image_url is not None:
                group['image_url'] = urlparse.urljoin(source_site_url, image_url)
            package['groups'].append(group)

    package['license_id'] = conv.check(conv.pipe(
        conv.test_in(license_id_translations),
        conv.translate(license_id_translations),
        ))(package['license_id'], state = state)

    organization = package.get('organization')
    if organization is not None:
        package['organization'] = organization = organization.copy()
        image_url = organization.get('image_url')
        if image_url is not None:
            organization['image_url'] = urlparse.urljoin(source_site_url, image_url)

    resources = package.get('resources')
    if resources is not None:
        package['resources'] = []
        for resource in resources:
            resource = resource.copy()

            conv.check(conv.pipe(
                conv.test_in([
                    u'EPSG:3949: RGF93 / CC49',
                    ]),
                ))(resource.pop('coord_referentiel', None), state = state)

            conv.check(conv.pipe(
                conv.test_in([
                    u'ISO8859-15',
                    u'UTF-8',
                    ]),
                ))(resource.pop('encoding', None), state = state)

            temporal_coverage_from = conv.check(conv.pipe(
                conv.input_to_int,
                conv.function(datetime.date.fromtimestamp),
                conv.date_to_iso8601_str
                ))(resource.pop('etendue_temp_debut', None), state = state)
            if temporal_coverage_from is not None:
                package['temporal_coverage_from'] = temporal_coverage_from
            temporal_coverage_to = conv.check(conv.pipe(
                conv.input_to_int,
                conv.function(datetime.date.fromtimestamp),
                conv.date_to_iso8601_str
                ))(resource.pop('etendue_temp_fin', None), state = state)
            if temporal_coverage_to is not None:
                package['temporal_coverage_to'] = temporal_coverage_to

            format = conv.check(conv.pipe(
                conv.function(lambda format: format.upper()),
                conv.test_in([
                    u'CSV',
                    u'SHP',
                    u'SHAPEFILE',
                    u'XLS',
                    u'XSL',
                    ]),
                conv.translate({
                    u'SHAPEFILE': u'SHP',
                    u'XSL': u'XLS',
                    }),
                ))(resource.pop('file_encoding', None), state = state)
            if resource.get('format') is None:
                resource['format'] = format

            resource.pop('format_version', None)
            resource.pop('version', None)

            package['resources'].append(resource)

    package['type'] = conv.check(conv.pipe(
        conv.test_in(type_translations),
        conv.translate(type_translations),
        ))(package['type'], state = state)

    return package, None


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
        supplier_abbreviation = u'hds',
        supplier_title = u'Open Data Hauts-de-Seine',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    global source_site_url
#    source_front_site_url = u'http://opendata.hauts-de-seine.net/'
    source_site_url = u'http://api-opendata.hauts-de-seine.net/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_list'),
        headers = source_headers)
    response = urllib2.urlopen(request, '{}')  # CKAN 1.7 requires a POST.
    response_dict = json.loads(response.read())
    packages_source_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve packages from source.
    for package_source_name in packages_source_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_show'),
            headers = source_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                id = package_source_name,
                ))))  # CKAN 1.7 requires a POST.
        response_dict = json.loads(response.read())
        source_package = conv.check(conv.pipe(
            before_ckan_json_to_package,
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if source_package is None:
            continue

        extras = conv.check(conv.struct(
            dict(
                attribut_description = conv.make_input_to_json(),
                author = conv.pipe(
                    conv.make_input_to_json(),
                    conv.test_isinstance(list),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                dict(
                                    author_email = conv.input_to_email,
                                    author_name = conv.cleanup_line,
                                    author_role = conv.noop,  # This value is ignored.
                                    ),
                                ),
                            ),
                        drop_none_items = True,
                        ),
                    conv.empty_to_none,
                    ),
                data_classification = conv.make_input_to_json(),
                dataset_language = conv.make_input_to_json(),
                degre_conformite_inspire = conv.make_input_to_json(),
                georeferencing_resource = conv.make_input_to_json(),
                inspire_category = conv.make_input_to_json(),
                iso_1911_category = conv.make_input_to_json(),
                legal_access_constraints = conv.make_input_to_json(),
                limit_range = conv.make_input_to_json(),
                metadata_access_limitation = conv.make_input_to_json(),
                metadata_date = conv.make_input_to_json(),
                systeme_reference_temporelle = conv.make_input_to_json(),
                thematic = conv.make_input_to_json(),
                ),
            ))(dict(
                (extra['key'], extra['value'])
                for extra in (source_package.get('extras') or [])
                ), state = conv.default_state)

        package = dict(
            license_id = source_package.get('license_id'),
            notes = source_package.get('notes'),
            title = source_package['title'],
            resources = [
                dict(
                    created = resource['created'],
                    format = resource.get('format'),
                    last_modified = resource.get('last_modified'),
                    name = resource['name'],
                    url = resource['url'],
                    )
                for resource in (source_package.get('resources') or [])
                ],
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(
                    strings.slugify(tag['name'])
                    for tag in (source_package.get('tags') or [])
                    ))
                if tag_name
                ],
            temporal_coverage_from = source_package.get('temporal_coverage_from'),
            temporal_coverage_to = source_package.get('temporal_coverage_to'),
            territorial_coverage = u'DepartmentOfFrance/92/92 HAUTS DE SEINE',
            url = urlparse.urljoin(source_site_url, 'dataset/{}'.format(source_package['name'])),
            )

        if extras['author'] is not None:
            author = extras['author'][0]
            if author['author_email'] is not None:
                package['author_email'] = author['author_email']
            if author['author_name'] is not None:
                package['author'] = author['author_name']

        if not args.dry_run:
            groups = source_package.pop('groups', None)
            organization = source_package.pop('organization', None)
            if groups is not None:
                groups = [
                    harvester.upsert_group(dict(
                        # Don't reuse image and description of groups, because Etalab has its own.
                        # description = group.get(u'description'),
                        # image_url = group.get(u'image_url'),
                        title = group[u'title'],
                        ))
                    for group in groups
                    ]
            if organization is None:
                organization = harvester.supplier
            else:
                organization = harvester.upsert_organization(dict(
                    description = organization.get(u'description'),
                    image_url = organization.get(u'image_url'),
                    title = organization[u'title'],
                    ))

        log.info(u'Harvested package: {}'.format(source_package['title']))
        if not args.dry_run:
            harvester.add_package(package, organization, source_package['name'], package['url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
