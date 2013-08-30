#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-CKAN-Harvesters -- Harvesters for Etalab's CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://gitorious.org/etalab/etalab-ckan-harvesters
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


"""Harvest NosDonnées.fr from RegardsCitoyens.org

http://www.nosdonnees.fr/
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


def after_ckan_json_to_package(package, state = None):
    if package is None:
        return package, None
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
    supplier_name = u'regards-citoyens'
    source_site_url = u'http://www.nosdonnees.fr/'
    target_site_url = conf['ckan.site_url']

    # Retrieve target organization (that will contain all harvested datasets).
    request = urllib2.Request(urlparse.urljoin(target_site_url,
        '/api/3/action/organization_show?id={}'.format(supplier_name)), headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    supplier = conv.check(conv.pipe(
        conv.make_ckan_json_to_organization(drop_none_values = True),
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)
    existing_packages_name = set(
        package['name']
        for package in (supplier.get('packages') or [])
        )

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, '/api/3/action/package_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request, '{}')  # CKAN 1.8 requires a POST.
    response_dict = json.loads(response.read())
    packages_source_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve packages from source.
    package_by_name = {}
    package_source_name_by_name = {}
    for package_source_name in packages_source_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, '/api/3/action/package_show'),
            headers = ckan_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                id = package_source_name,
                ))))  # CKAN 1.8 requires a POST.
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if package is None:
            continue
        if package is None:
            continue
        package['owner_org'] = supplier['id']
        package_groups = package.pop('groups', None)
        tags = [
            dict(
                name = group['name'],
                )
            for group in (package_groups or [])
            ]
        if tags:
            package.setdefault('tags', []).extend(tags)
        helpers.set_extra(package, 'harvest_app_name', app_name)
        helpers.set_extra(package, 'supplier_id', supplier['id'])
        package_name = strings.slugify(package['title'])[:100]
        package_source_name_by_name[package_name] = package['name']
        package['name'] = package_name
        package_by_name[package_name] = package
        log.info(u'Harvested package: {}'.format(package['title']))

    # Upsert source packages to target.
    for package_name, package in package_by_name.iteritems():
        if package_name in existing_packages_name:
            log.info(u'Updating package: {}'.format(package['title']))
            existing_packages_name.remove(package_name)
            request = urllib2.Request(urlparse.urljoin(target_site_url,
                '/api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
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
            request = urllib2.Request(urlparse.urljoin(target_site_url, '/api/3/action/package_create'),
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
                        '/api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
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
            '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
        package_by_name[package_name] = package

    supplier_package_title = u'Jeux de données - {}'.format(supplier['title'])
    supplier_package_name = strings.slugify(supplier_package_title)[:100]
    log.info(u'Upserting package: {}'.format(supplier_package_name))
    supplier_packages_file = cStringIO.StringIO()
    supplier_packages_csv_writer = csv.writer(supplier_packages_file, delimiter = ';', quotechar = '"',
        quoting = csv.QUOTE_MINIMAL)
    supplier_packages_csv_writer.writerow([
        'Titre',
        'Nom',
        'Nom original',
        ])
    for package_name, package in sorted(package_by_name.iteritems()):
        supplier_packages_csv_writer.writerow([
            package['title'].encode('utf-8'),
            package['name'].encode('utf-8'),
            package_source_name_by_name[package['name']].encode('utf-8'),
            ])
    file_metadata = filestores.upload_file(target_site_url, supplier_package_name,
        supplier_packages_file.getvalue(), ckan_headers)

    supplier_package = dict(
        author = supplier['title'],
        extras = [
            dict(
                key = 'harvest_app_name',
                value = app_name,
                ),
            ],
        license_id = 'odbl',
        name = supplier_package_name,
        notes = u'''\
Les jeux de données fournis par {} pour data.gouv.fr.
'''.format(supplier['title']),
        owner_org = supplier['id'],
        resources = [
            dict(
                created = file_metadata['_creation_date'],
                format = 'CSV',
                hash = file_metadata['_checksum'],
                last_modified = file_metadata['_last_modified'],
                name = supplier_package_name + u'.txt',
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
        title = supplier_package_title,
        )
    existing_packages_name.discard(supplier_package_name)
    upsert_package(target_site_url, supplier_package)

    # Delete obsolete packages.
    for package_name in existing_packages_name:
        # Retrieve package id (needed for delete).
        log.info(u'Deleting package: {}'.format(package_name))
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_package = response_dict['result']

        request = urllib2.Request(urlparse.urljoin(target_site_url,
            '/api/3/action/package_delete?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
        response_dict = json.loads(response.read())
#        deleted_package = response_dict['result']
#        pprint.pprint(deleted_package)

    return 0


def upsert_package(target_site_url, package):
    package['name'] = name = strings.slugify(package['title'])[:100]

    request = urllib2.Request(urlparse.urljoin(target_site_url,
        '/api/3/action/package_show?id={}'.format(name)), headers = ckan_headers)
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
        request = urllib2.Request(urlparse.urljoin(target_site_url, '/api/3/action/package_create'),
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
            '/api/3/action/package_update?id={}'.format(name)), headers = ckan_headers)
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
