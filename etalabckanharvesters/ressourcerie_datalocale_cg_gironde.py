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


"""Harvest "La Ressourcerie Datalocale du Conseil Général de Gironde".

http://catalogue.datalocale.fr/
"""


import argparse
import ConfigParser
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states
from ckantoolbox import ckanconv

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(app_name)


def after_ckan_json_to_group(group, state = None):
    if group is None:
        return group, None
    group = group.copy()

    if group.get('extras'):
        extras = []
        for extra in group['extras']:
            value = extra.get('value')
            if value is None:
                continue
            value = json.loads(value)
            if value in (None, ''):
                continue
            # Add a new extra with only key and value.
            extras.append(dict(
                key = extra['key'],
                value = value,
                ))
        group['extras'] = extras or None

    if group.get('groups'):
        sub_groups = []
        for sub_group in group['groups']:
            sub_group = sub_group.copy()
            if sub_group.pop('capacity', None) == u'private':
                continue
            sub_group.pop('revision_id', None)
            sub_groups.append(sub_group)
        group['groups'] = sub_groups

    if not group.get('packages'):
        return None, None

    if group.get('private', False) or group.get('capacity') == u'private':
        return None, None

    group.pop('capacity', None)
    group.pop('id', None)  # Don't reuse source ID in target.
    group.pop('revision_id', None)
    group.pop('users', None)  # Don't reuse source users in target.

    return group, None


def after_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
    package = package.copy()

    if package.get('extras'):
        extras = []
        for extra in package['extras']:
            key = extra['key']
            if key in (
                    'ckan_author',  # Ignore source ID of author.
                    'dct:publisher',  # Ignore source ID of publisher.
                    ):
                continue
            new_key, value_converter = {
                'dataQuality': (u"Qualité des données", conv.cleanup_line),
                'dc:source': (u"Source", conv.cleanup_line),
                'dcat:granularity': (u"Granularité des données", conv.pipe(
                    conv.cleanup_line,
#                    conv.test_in([
#                        u'1/10000',
#                        u'1:20000',
#                        u'1/25000',
#                        u'1:50000',
#                        u"canton",
#                        u"chaque événement est un item",
#                        u"commune",
#                        u"comptage",
#                        u"émissions",
#                        u"horaires théoriques",
#                        u"ilot iris",
#                        u"individu",
#                        u"jeu de données",
#                        u"m3",
#                        u"minute",
#                        u"nombre d'individus supérieur a 5",
#                        u"nombre de demandeur supérieur a 5",
#                        u"point d'intérêt",
#                        u"points d'intérêt",
#                        u"polyligne",
#                        u'pourcentage',
#                        u"structure d'hébergement",
#                        u"surface agricole en hectares",
#                        ]),
                    )),
                'dct:accrualPeriodicity': (u"Fréquence de mise à jour", conv.pipe(
                    conv.cleanup_line,
                    conv.translate({
                        u'Irrégulier': u"ponctuelle",
                        u'irrégulier': u"ponctuelle",
                        u'irrégulière': u"ponctuelle",
                        u'Irrégulière': u"ponctuelle",
                        u"journalier": u"quotidienne",
                        u'Tous les 5 ans': u"quinquennale",
                        }),
                    conv.test_in([
                        u"annuelle",
                        u"bimensuelle",
                        u"bimestrielle",
                        u"hebdomadaire",
                        u"quinquennale"
                        u"mensuelle",
                        u"ponctuelle",
                        u"quotidienne",
                        u"semestrielle",
                        u"temps réel",
                        u"trimestrielle",
                        ]),
                    )),
                'dcterms:references': (u"Références", conv.cleanup_line),
                }.get(key, (None, conv.cleanup_line))
            if new_key not in (None, key):
                key = new_key
            value = extra.get('value')
            if value is not None:
                value = json.loads(value)
            value, error = value_converter(value, state = conv.default_state)
            if error is not None:
                log.warning(u"{}: {}. Error: {}".format(key, value, error))
            if value is None:
                continue
            # Add a new extra with only key and value.
            extras.append(dict(
                key = extra['key'],
                value = value,
                ))
        package['extras'] = extras or None

    if package.get('groups'):
        groups = []
        for group in package['groups']:
            group = group.copy()
            if group.pop('capacity', None) == u'private':
                continue
            group.pop('revision_id', None)
            groups.append(group)
        package['groups'] = groups

    if package.get('private', False) or package.get('capacity') == u'private':
        return None, None

    package.pop('capacity', None)
    del package['id']  # Don't reuse source ID in target.
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

    return package, None


def before_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
    package = package.copy()
    if package.get('license_id') == u'lo-ol':
        package['license_id'] = u'fr-lo'

    # Remove fields that are also in extras (in another form).
    for key in (
            u'dcat:granularity',  # Needed because key will be removed from extras below.
            u'dct:accrualPeriodicity',
            u'dct:accrualPeriodicity-other',
            u'dct:contributor',  # Needed because key will be removed from extras below.
            u'geographic_granularity',
            u'geographic_granularity-other',
            ):
        package.pop(key, None)

    # Put some extras fields into main fields.
    if package.get('extras'):
        package['extras'] = package['extras'][:]

    value = helpers.get_extra(package, 'dcat:granularity', None)
    if value is not None:
        value = json.loads(value)
        value = {
            u"canton": u"canton",
            u"commune": u"commune",
            u"ilot iris": u"commune",
            u"point d'intérêt": u"commune",
            u"points d'intérêt": u"commune",
            }.get(value)
        if value is not None:
            helpers.pop_extra(package, 'dcat:granularity')
            assert package.get('territorial_coverage_granularity') is None, package
            package['territorial_coverage_granularity'] = value

    value = helpers.pop_extra(package, 'dct:contributor', None)
    if value is not None:
        value = json.loads(value)
    if value is not None:
        package['maintainer'] = value

    # Put extension fields into extras.

    value = package.pop(u'dataQuality', None)
    if value:
        for item in value:
            package.setdefault('extras', []).append(dict(
                key = u'dataQuality',
                value = json.dumps(item),
                ))

    value = package.pop(u'theme_available', None)
    if value:
        for item in value:
            package.setdefault('extras', []).append(dict(
                key = u'theme_available',
                value = json.dumps(item),
                ))

    value = package.pop(u'themeTaxonomy', None)
    if value:
        for item in value:
            package.setdefault('extras', []).append(dict(
                key = u'themeTaxonomy',
                value = json.dumps(item),
                ))

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

    excluded_organizations_name = set([
        u'atos',
        u'open-street-map',
        ])
    harvester = helpers.Harvester(
        supplier_abbreviation = u'rdl',
        supplier_title = u'Ressourcerie datalocale',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://catalogue.datalocale.fr/'

    harvester.retrieve_target()

    # Retrieve names of groups in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/group_list'),
        headers = source_headers)
    response = urllib2.urlopen(request, '{}')  # CKAN 1.7 requires a POST.
    response_dict = json.loads(response.read())
    groups_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve all groups from source.
    group_by_name = {}
    for group_name in groups_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/group_show'),
            headers = source_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
            id = group_name,
            ))))  # CKAN 1.7 requires a POST.
        response_dict = json.loads(response.read())
        group = conv.check(conv.pipe(
            conv.make_ckan_json_to_group(drop_none_values = True),
            conv.not_none,
            after_ckan_json_to_group,
            ))(response_dict['result'], state = conv.default_state)
        if group is None:
            continue
        group_by_name[group_name] = group

    # Create or update organizations.
    organization_by_source_name = {}
    for group_name, group in sorted(group_by_name.iteritems()):
        if group.get('type') != 'organization':
            continue
        if group_name in excluded_organizations_name:
            continue
        organization = dict(
            title = group['title'],
            )
        if group.get('description') is not None:
            organization['description'] = group['description']
        if group.get('image_url') is not None:
            organization['image_url'] = group['image_url']
        organization_by_source_name[group_name] = organization

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
        package = conv.check(conv.pipe(
            before_ckan_json_to_package,
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if package is None:
            continue
        for group in (package.get('groups') or []):
            if group['name'] in excluded_organizations_name:
                # Don't import packages from excluded organizations.
                log.info(u'Ignoring harvested package: {}'.format(package['title']))
                package = None
                break
            organization = organization_by_source_name.get(group['name'])
            if organization is not None:
                organization = harvester.upsert_organization(organization)
                break
        else:
            log.warning(u'''Package: "{}" doesn't belong to any organization group, but to {}'''.format(
                package['title'],
                u', '.join(
                    u'{} ({})'.format(group['name'], group['title']) if group.get('title') else group['name']
                    for group in (package.get('groups') or [])
                    ),
                ))
            organization = harvester.supplier
        if package is None:
            continue
        frequency = helpers.pop_extra(package, u"Fréquence de mise à jour", default = None)
        if frequency is not None:
            package['frequency'] = frequency
        package.pop('groups', None)
        source_name = package.pop('name')
        package.pop('users', None)

        source_url = urlparse.urljoin(source_site_url, 'dataset/{}'.format(source_name))
        helpers.set_extra(package, u'Source', source_url)

        package = conv.check(conv.ckan_input_package_to_output_package)(package, state = conv.default_state)
        log.info(u'Harvested package: {}'.format(package['title']))
        harvester.add_package(package, organization, source_name, source_url)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
