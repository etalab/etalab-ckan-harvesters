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


"""Harvest "La Ressourcerie Datalocale du Conseil Général de Gironde".

http://catalogue.datalocale.fr/
"""


import argparse
import ConfigParser
import cStringIO
import csv
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv, filestores

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
ckan_headers = None
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
                        u'Irrégulier': u"au fil de l'eau",
                        u'irrégulier': u"au fil de l'eau",
                        u'irrégulière': u"au fil de l'eau",
                        u'Irrégulière': u"au fil de l'eau",
                        u'Tous les 5 ans': u"quinquénale",
                        }),
                    conv.test_in([
                        u"annuelle",
                        u"au fil de l'eau",
                        u"bimensuelle",
                        u"bimestrielle",
                        u"hebdomadaire",
                        u"journalier",
                        u"quinquénale"
                        u"mensuelle",
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


def group_to_organization_ckan_json(group, state = None):
    if group is None:
        return None, None
    organization = group.copy()
    organization.pop('groups', None)
    return organization, None


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

    global ckan_headers
    ckan_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }
    excluded_organizations_name = set([
        u'atos',
        u'open-street-map',
        ])
    organization_by_name = {}
    supplier_name = u'ressourcerie-datalocale'
    source_site_url = u'http://catalogue.datalocale.fr/'
    target_site_url = conf['ckan.site_url']

    # Retrieve target organization (that will contain all harvested datasets).
    request = urllib2.Request(urlparse.urljoin(target_site_url,
        'api/3/action/organization_show?id={}'.format(supplier_name)), headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    supplier = conv.check(conv.pipe(
        conv.make_ckan_json_to_organization(drop_none_values = True),
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)
    organization_by_name[supplier_name] = supplier

    existing_packages_name = set()
    for organization_package in (supplier.get('packages') or []):
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(organization_package['name'])), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        organization_package = conv.check(
            conv.make_ckan_json_to_package(drop_none_values = True),
            )(response_dict['result'], state = conv.default_state)
        if organization_package is None:
            continue
        for tag in (organization_package.get('tags') or []):
            if tag['name'] == 'liste-de-jeux-de-donnees':
                break
        else:
            # This dataset doesn't contain a list of datasets. Ignore it.
            continue
        existing_packages_name.add(organization_package['name'])
        for resource in (organization_package.get('resources') or []):
            response = urllib2.urlopen(resource['url'])
            packages_csv_reader = csv.reader(response, delimiter = ';', quotechar = '"')
            packages_csv_reader.next()
            for row in packages_csv_reader:
                package_infos = dict(
                    (key, value.decode('utf-8'))
                    for key, value in zip(['title', 'name', 'source_name'], row)
                    )
                existing_packages_name.add(package_infos['name'])

    # Retrieve names of groups in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/group_list'),
        headers = ckan_headers)
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
            headers = ckan_headers)
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
        organization = conv.check(group_to_organization_ckan_json)(group, state = conv.default_state)
        helpers.set_extra(organization, 'harvest_app_name', app_name)
        log.info(u'Upserting organization: {}'.format(organization['title']))
        organization = helpers.upsert_organization(target_site_url, organization, headers = ckan_headers)
        organization_by_name[organization['name']] = organization
        organization_by_source_name[group_name] = organization

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request, '{}')  # CKAN 1.7 requires a POST.
    response_dict = json.loads(response.read())
    packages_source_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve packages from source.
    organization_name_by_package_name = {}
    package_by_name = {}
    package_source_name_by_name = {}
    for package_source_name in packages_source_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_show'),
            headers = ckan_headers)
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
                break
        else:
            log.warning(u'''Package: "{}" doesn't belong to any organization group, but to {}'''.format(
                package['title'],
                u', '.join(
                    u'{} ({})'.format(group['name'], group['title']) if group.get('title') else group['name']
                    for group in (package.get('groups') or [])
                    ),
                ))
            organization = supplier
        if package is None:
            continue
        package['owner_org'] = organization['id']
        package.pop('groups', None)
        helpers.set_extra(package, 'harvest_app_name', app_name)
        helpers.set_extra(package, 'supplier_id', supplier['id'])
        package_name = strings.slugify(package['title'])[:100]
        package_source_name_by_name[package_name] = package['name']
        package['name'] = package_name
        package_by_name[package_name] = package
        organization_name_by_package_name[package_name] = organization['name']
        log.info(u'Harvested package: {}'.format(package['title']))

    # Upsert source packages to target.
    packages_by_organization_name = {}
    for package_name, package in package_by_name.iteritems():
        if package_name in existing_packages_name:
            log.info(u'Updating package: {}'.format(package['title']))
            existing_packages_name.remove(package_name)
            request = urllib2.Request(urlparse.urljoin(target_site_url,
                'api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'An exception occured while updating package: {}'.format(package))
                    log.error(response_text)
                    continue
                log.error(u'An error occured while updating package: {}'.format(package))
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                updated_package = response_dict['result']
#                pprint.pprint(updated_package)
        else:
            log.info(u'Creating package: {}'.format(package['title']))
            request = urllib2.Request(urlparse.urljoin(target_site_url, 'api/3/action/package_create'),
                headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'An exception occured while creating package: {}'.format(package))
                    log.error(response_text)
                    continue
                error = response_dict.get('error', {})
                if error.get('__type') == u'Validation Error' and error.get('name'):
                    # A package with the same name already exists. Maybe it is deleted. Undelete it.
                    package['state'] = 'active'
                    request = urllib2.Request(urlparse.urljoin(target_site_url,
                        'api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
                    try:
                        response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
                    except urllib2.HTTPError as response:
                        response_text = response.read()
                        try:
                            response_dict = json.loads(response_text)
                        except ValueError:
                            log.error(u'An exception occured while undeleting package: {}'.format(package))
                            log.error(response_text)
                            continue
                        log.error(u'An error occured while undeleting package: {}'.format(package))
                        for key, value in response_dict.iteritems():
                            print '{} = {}'.format(key, value)
                    else:
                        assert response.code == 200
                        response_dict = json.loads(response.read())
                        assert response_dict['success'] is True
#                        updated_package = response_dict['result']
#                        pprint.pprint(updated_package)
                else:
                    log.error(u'An error occured while creating package: {}'.format(package))
                    for key, value in response_dict.iteritems():
                        print '{} = {}'.format(key, value)
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                created_package = response_dict['result']
#                pprint.pprint(created_package)

        # Read updated package.
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
        packages_by_organization_name.setdefault(organization_name_by_package_name[package_name], []).append(package)

    for organization_name, organization in organization_by_name.iteritems():
        organization_package_title = u'Jeux de données - {}'.format(organization['title'])
        organization_package_name = strings.slugify(organization_package_title)[:100]
        existing_packages_name.discard(organization_package_name)
        organization_packages = packages_by_organization_name.get(organization_name)
        if organization_packages:
            log.info(u'Upserting package: {}'.format(organization_package_name))
            organization_packages_file = cStringIO.StringIO()
            organization_packages_csv_writer = csv.writer(organization_packages_file, delimiter = ';', quotechar = '"',
                quoting = csv.QUOTE_MINIMAL)
            organization_packages_csv_writer.writerow([
                'Titre',
                'Nom',
                'Nom original',
                ])
            for organization_package in organization_packages:
                organization_packages_csv_writer.writerow([
                    organization_package['title'].encode('utf-8'),
                    organization_package['name'].encode('utf-8'),
                    package_source_name_by_name[organization_package['name']].encode('utf-8'),
                    ])
            file_metadata = filestores.upload_file(target_site_url, organization_package_name,
                organization_packages_file.getvalue(), ckan_headers)

            organization_package = dict(
                author = supplier['title'],
                extras = [
                    dict(
                        key = 'harvest_app_name',
                        value = app_name,
                        ),
                    ],
                license_id = 'odbl',
                name = organization_package_name,
                notes = u'''\
Les jeux de données fournis par {} pour data.gouv.fr.
'''.format(organization['title']),
                owner_org = supplier['id'],
                resources = [
                    dict(
                        created = file_metadata['_creation_date'],
                        format = 'CSV',
                        hash = file_metadata['_checksum'],
                        last_modified = file_metadata['_last_modified'],
                        name = organization_package_name + u'.txt',
                        size = file_metadata['_content_length'],
                        url = file_metadata['_location'],
#                        revision_id – (optional)
#                        description (string) – (optional)
#                        resource_type (string) – (optional)
#                        mimetype (string) – (optional)
#                        mimetype_inner (string) – (optional)
#                        webstore_url (string) – (optional)
#                        cache_url (string) – (optional)
#                        cache_last_updated (iso date string) – (optional)
#                        webstore_last_updated (iso date string) – (optional)
                        ),
                    ],
                tags = [
                    dict(
                        name = 'liste-de-jeux-de-donnees',
                        ),
                    ],
                title = organization_package_title,
                )
            upsert_package(target_site_url, organization_package)
        else:
            # Delete dataset if it exists.
            log.info(u'Deleting package: {}'.format(organization_package_name))

            # Retrieve package id (needed for delete).
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                'api/3/action/package_show?id={}'.format(organization_package_name)), headers = ckan_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            existing_package = response_dict['result']

            # TODO: To replace with package_purge when it is available.
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                'api/3/action/package_delete?id={}'.format(organization_package_name)), headers = ckan_headers)
            response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
            response_dict = json.loads(response.read())
#            deleted_package = response_dict['result']
#            pprint.pprint(deleted_package)

    # Delete obsolete packages.
    for package_name in existing_packages_name:
        # Retrieve package id (needed for delete).
        log.info(u'Deleting package: {}'.format(package_name))
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_package = response_dict['result']

        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_delete?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
        response_dict = json.loads(response.read())
#        deleted_package = response_dict['result']
#        pprint.pprint(deleted_package)

    return 0


def upsert_package(target_site_url, package):
    package['name'] = name = strings.slugify(package['title'])[:100]

    request = urllib2.Request(urlparse.urljoin(target_site_url,
        'api/3/action/package_show?id={}'.format(name)), headers = ckan_headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as response:
        if response.code != 404:
            raise
        existing_package = {}
    else:
        response_text = response.read()
        try:
            response_dict = json.loads(response_text)
        except ValueError:
            log.error(u'An exception occured while reading package: {0}'.format(package))
            log.error(response_text)
            raise
        existing_package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
    if existing_package.get('id') is None:
        # Create package.
        request = urllib2.Request(urlparse.urljoin(target_site_url, 'api/3/action/package_create'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while creating package: {0}'.format(package))
                log.error(response_text)
                raise
            log.error(u'An exception occured while creating package: {0}'.format(package))
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            created_package = response_dict['result']
#            pprint.pprint(created_package)
            package['id'] = created_package['id']
    else:
        # Update package.
        package['id'] = existing_package['id']
        package['state'] = 'active'

        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_update?id={}'.format(name)), headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while updating package: {0}'.format(package))
                log.error(response_text)
                raise
            log.error(u'An exception occured while updating package: {0}'.format(package))
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
#            updated_package = response_dict['result']
#            pprint.pprint(updated_package)
    return package


if __name__ == '__main__':
    sys.exit(main())
