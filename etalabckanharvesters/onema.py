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
from owslib.csw import CatalogueServiceWeb

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
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

    organization_by_name = {}
    source_site_url = u'http://opendata-sie-back.brgm-rec.fr/geosource/srv/eng/csw'  # Recette environment
    supplier_name = u'office-national-de-l-eau-et-des-milieux-aquatiques'
    target_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }
    target_site_url = conf['ckan.site_url']

    # Retrieve target organization (that will contain all harvested datasets).
    request = urllib2.Request(urlparse.urljoin(target_site_url,
        'api/3/action/organization_show?id={}'.format(supplier_name)), headers = target_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    supplier = conv.check(conv.pipe(
        conv.make_ckan_json_to_organization(drop_none_values = True),
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)
    organization_by_name[supplier_name] = supplier

    existing_packages_name = set()
    for organization_package in (supplier.get('packages') or []):
        if not organization_package['name'].startswith('jeux-de-donnees-'):
            continue
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(organization_package['name'])), headers = target_headers)
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
    organization_name_by_package_name = {}
    package_by_name = {}
    for record_id in record_by_id.iterkeys():
        csw.getrecordbyid(id = [record_id])
        record = csw.records[record_id]

        package_name = strings.slugify(record.title)[:100]
        package = dict(
            license_id = u'fr-lo',
            name = package_name,
            notes = record.abstract,
            owner_org = supplier['id'],
            resources = [
                dict(
                    description = uri.get('description') or None,
                    format = record.format,
                    url = uri['url'],
                    )
                for uri in record.uris
                ],
            supplier_id = supplier['id'],
            tags = [
                dict(name = strings.slugify(subject))
                for subject in record.subjects
                ],
#            territorial_coverage = TODO
            title = record.title,
            )
        helpers.set_extra(package, 'harvest_app_name', app_name)

        organization_name_by_package_name[package_name] = supplier['name']
        package_by_name[package_name] = package

        log.info(u'Harvested package: {}'.format(package['title']))

    # Upsert source packages to target.
    packages_by_organization_name = {}
    for package_name, package in package_by_name.iteritems():
        if package_name in existing_packages_name:
            log.info(u'Updating package: {}'.format(package['title']))
            existing_packages_name.remove(package_name)
            request = urllib2.Request(urlparse.urljoin(target_site_url,
                'api/3/action/package_update?id={}'.format(package_name)), headers = target_headers)
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
                headers = target_headers)
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
                        'api/3/action/package_update?id={}'.format(package_name)), headers = target_headers)
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
            'api/3/action/package_show?id={}'.format(package_name)), headers = target_headers)
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
#                    package_source_name_by_name[organization_package['name']].encode('utf-8'),
                    organization_package['title'].encode('utf-8'),
                    ])
            file_metadata = filestores.upload_file(target_site_url, organization_package_name,
                organization_packages_file.getvalue(), target_headers)

            organization_package = dict(
                author = supplier['title'],
                extras = [
                    dict(
                        key = 'harvest_app_name',
                        value = app_name,
                        ),
                    ],
                license_id = 'fr-lo',
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
            helpers.upsert_package(target_site_url, organization_package, headers = target_headers)
        else:
            # Delete dataset if it exists.
            log.info(u'Deleting package: {}'.format(organization_package_name))

            # Retrieve package id (needed for delete).
            request = urllib2.Request(urlparse.urljoin(target_site_url,
                'api/3/action/package_show?id={}'.format(organization_package_name)), headers = target_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            existing_package = response_dict['result']

            # TODO: To replace with package_purge when it is available.
            request = urllib2.Request(urlparse.urljoin(target_site_url,
                'api/3/action/package_delete?id={}'.format(organization_package_name)), headers = target_headers)
            response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
            response_dict = json.loads(response.read())
#            deleted_package = response_dict['result']
#            pprint.pprint(deleted_package)

    # Delete obsolete packages.
    for package_name in existing_packages_name:
        # Retrieve package id (needed for delete).
        log.info(u'Deleting package: {}'.format(package_name))
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(package_name)), headers = target_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_package = response_dict['result']

        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_delete?id={}'.format(package_name)), headers = target_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
        response_dict = json.loads(response.read())
#        deleted_package = response_dict['result']
#        pprint.pprint(deleted_package)

    return 0


if __name__ == '__main__':
    sys.exit(main())
