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


"""Harvest Lutèce from City of Paris

http://opendata.paris.fr/opendata/rest/ckan/...
http://dev.lutece.paris.fr/plugins/module-document-ckan/
"""


import argparse
import ConfigParser
import datetime
import json
import logging
import os
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(app_name)
now_str = datetime.datetime.now().isoformat()
today_str = datetime.date.today().isoformat()


def after_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
    if state is None:
        state = conv.default_state
    errors = {}
    package = package.copy()

    if package.get('extras'):
        extras = []
        for extra in package['extras']:
            value = extra.get('value')
            if value is not None:
                value = json.loads(value)
            if value in (None, ''):
                continue
            # Add a new extra with only key and value.
            extras.append(dict(
                key = extra['key'],
                value = value,
                ))
        package['extras'] = extras or None

    if package.get('private', False) or package.get('capacity') == u'private':
        return None, None

    package.pop('capacity', None)
    del package['id']  # Don't reuse source ID in target.
    organization = package.pop('organization', None)
    if organization is None:
        errors['organization'] = state._(u'Missing value')
    elif organization.get('id') is None:
        errors['organization'] = dict(id = state._(u'Missing value'))
    elif organization['id'] != '1':
        errors['organization'] = dict(
            id = state._(u'Unexpected organization ID. TODO: Add support for several organization'),
            )
    package.pop('revision_id', None)
    package.pop('users', None)  # Don't reuse source users in target.

    if package.get('resources'):
        resources = []
        for resource in package['resources']:
            resource = resource.copy()
            if resource.pop('capacity', None) == u'private':
                continue
            resource.pop('revision_id', None)
            resources.append(resource)
        package['resources'] = resources

    return package, errors or None


def before_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
    package = package.copy()

    for key in ('metadata_created', 'metadata_modified'):
        value = package.get(key)
        if value is None:
            package[key] = datetime.date.today().isoformat()

    organization = package.get('organization')
    if organization is not None:
        package['organization'] = organization = organization.copy()

        for key in ('created',):
            value = organization.get(key)
            if value is None:
                organization[key] = datetime.date.today().isoformat()

        if organization.get('revision_id') is None:
            organization['revision_id'] = 'latest'

        for key in ('revision_timestamp',):
            value = organization.get(key)
            if value is None:
                organization[key] = now_str

    resources = package.get('resources')
    if resources:
        package['resources'] = resources = resources[:]

        for resource_index, resource in enumerate(resources):
            resources[resource_index] = resource = resource.copy()

            for key in ('created',):
                value = resource.get(key)
                if value is None:
                    resource[key] = datetime.date.today().isoformat()

            if resource.get('id') is None:
                resource['id'] = u'{}-{}'.format(package['id'], resource_index)

            if resource.get('revision_id') is None:
                resource['revision_id'] = 'latest'

    if package.get('revision_id') is None:
        package['revision_id'] = 'latest'

    for key in ('revision_timestamp',):
        value = package.get(key)
        if value is None:
            package[key] = now_str

    tags = package.get('tags')
    if tags:
        package['tags'] = tags = tags[:]

        for tag_index, tag in enumerate(tags):
            tags[tag_index] = tag = tag.copy()

            if tag.get('id') is None:
                tag['id'] = tag['name']

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

    config_parser = ConfigParser.SafeConfigParser(dict(
        here = os.path.dirname(os.path.abspath(os.path.normpath(args.config))),
        ))
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
        supplier_abbreviation = u'prs',
        supplier_title = u'Mairie de Paris',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://opendata.paris.fr/opendata/rest/ckan/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_list'), headers = source_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read(), encoding = 'cp1252')
    packages_source_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve packages from source.
    for package_source_name in packages_source_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, u'api/3/action/package_show?id={}'.format(
            package_source_name)).encode('utf-8'), headers = source_headers)
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, response:
            if response.code == 404:
                log.warning(u'Skipping package {}, because page not found'.format(package_source_name))
                continue
            raise
        response_dict = json.loads(response.read())
        if not response_dict['success']:
            log.warning(u'Skipping package {}, because {}'.format(package_source_name, response_dict))
            continue
        source_package = conv.check(conv.pipe(
            before_ckan_json_to_package,
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if source_package is None:
            continue

        package = dict(
#            frequency = source_package.get('frequency'),
            license_id = {
                u'ODbL': u'odc-odbl',
                }[source_package.get('license_id')],
            notes = source_package.get('notes'),
            title = source_package['title'],
            resources = [
                dict(
                    created = resource['created'],
                    format = resource.get('format'),
                    last_modified = resource.get('last_modified'),
                    name = resource.get('name') or u'Fichier.{}'.format(resource.get('format')).strip(u'.'),
                    url = resource['url'],
                    )
                for resource in (source_package.get('resources') or [])
                if resource.get('url') is not None
                ],
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(
                    strings.slugify(tag['name'])
                    for tag in (source_package.get('tags') or [])
                    ))
                if tag_name and len(tag_name) > 2
                ],
#            temporal_coverage_from = source_package.get('temporal_coverage_from'),
#            temporal_coverage_to = source_package.get('temporal_coverage_to'),
            territorial_coverage = u'CommuneOfFrance/75056/75000 PARIS',
            url = source_package['url'],
            )

        if not args.dry_run:
            groups = source_package.get('groups')
            if groups is not None:
                groups = [
                    harvester.upsert_group(dict(
                        # Don't reuse image and description of groups, because Etalab has its own.
                        # description = group.get(u'description'),
                        # image_url = group.get(u'image_url'),
                        title = group_title,
                        ))
                    for group_title in sorted(list(set(
                        group['title']
                        for group in (groups or [])
                        )) + [u"Territoires"])
                    ]

        log.info(u'Harvested package: {}'.format(package['title']))
        if not args.dry_run:
            harvester.add_package(package, harvester.supplier, source_package['name'], package['url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
