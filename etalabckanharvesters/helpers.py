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


"""Helpers for harvesters"""


import cStringIO
import csv
import itertools
import json
import logging
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv, filestores

conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(__name__)


class Harvester(object):
    existing_packages_name = None
    group_by_name = None
    old_supplier_name = None
    old_supplier_title = None
    organization_by_name = None
    organization_name_by_package_name = None
    package_by_name = None
    package_source_by_name = None
    packages_by_organization_name = None
    related_by_package_name = None
    supplier_abbreviation = None
    supplier = None
    supplier_name = None
    supplier_title = None
    target_headers = None
    target_site_url = None

    def __init__(self, old_supplier_title = None, supplier_abbreviation = None, supplier_title = None,
            target_headers = None, target_site_url = None):
        if old_supplier_title is not None:
            assert isinstance(old_supplier_title, unicode)
            self.old_supplier_title = old_supplier_title
            old_supplier_name = strings.slugify(old_supplier_title)
            assert old_supplier_name
            assert len(old_supplier_name) <= 100
            self.old_supplier_name = old_supplier_name

        assert isinstance(supplier_abbreviation, unicode)
        assert supplier_abbreviation == strings.slugify(supplier_abbreviation)
        assert 1 < len(supplier_abbreviation) < 5
        self.supplier_abbreviation = supplier_abbreviation

        assert isinstance(supplier_title, unicode)
        self.supplier_title = supplier_title
        supplier_name = strings.slugify(supplier_title)
        assert supplier_name
        assert len(supplier_name) <= 100
        self.supplier_name = supplier_name

        assert isinstance(target_headers, dict)
        assert isinstance(target_headers['Authorization'], basestring)
        assert isinstance(target_headers['User-Agent'], basestring)
        self.target_headers = target_headers

        assert isinstance(target_site_url, unicode)
        self.target_site_url = target_site_url

        self.existing_packages_name = set()
        self.group_by_name = {}
        self.organization_by_name = {}
        self.organization_name_by_package_name = {}
        self.package_by_name = {}
        self.package_source_by_name = {}
        self.packages_by_organization_name = {}
        self.related_by_package_name = {}

    def add_package(self, package, organization, source_name, source_url, groups = None, related = None):
        name = self.name_package(package['title'])
        if package.get('name') is None:
            package['name'] = name
        else:
            assert package['name'] == name, package

        package['groups'] = [
            dict(
                id = group['id'],
                )
            for group in (groups or [])
            ]
        package['owner_org'] = organization['id']
        package['supplier_id'] = self.supplier['id']

        assert name not in self.package_by_name
        self.package_by_name[name] = package

        assert name not in self.organization_name_by_package_name
        self.organization_name_by_package_name[name] = organization['name']

        assert name not in self.package_source_by_name
        self.package_source_by_name[name] = dict(
            name = source_name,
            url = source_url,
            )

        if related:
            self.related_by_package_name[name] = related

    def name_package(self, title):
        for index in itertools.count(1):
            differentiator = u'-{}'.format(index) if index > 1 else u''
            name = u'{}{}-{}'.format(
                strings.slugify(title)[:100 - len(self.supplier_abbreviation) - 1 - len(differentiator)].rstrip(u'-'),
                differentiator,
                self.supplier_abbreviation,
                )
            if name not in self.package_by_name:
                return name

    def retrieve_supplier_existing_packages(self, supplier):
        for package in (supplier.get('packages') or []):
            if not package['name'].startswith('jeux-de-donnees-'):
                continue
            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/package_show?id={}'.format(package['name'])), headers = self.target_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            package = conv.check(
                conv.make_ckan_json_to_package(drop_none_values = True),
                )(response_dict['result'], state = conv.default_state)
            if package is None:
                continue
            for tag in (package.get('tags') or []):
                if tag['name'] == 'liste-de-jeux-de-donnees':
                    break
            else:
                # This dataset doesn't contain a list of datasets. Ignore it.
                continue
            self.existing_packages_name.add(package['name'])
            for resource in (package.get('resources') or []):
                request = urllib2.Request(resource['url'], headers = self.target_headers)
                response = urllib2.urlopen(request)
                packages_csv_reader = csv.reader(response, delimiter = ';', quotechar = '"')
                packages_csv_reader.next()
                for row in packages_csv_reader:
                    package_infos = dict(
                        (key, value.decode('utf-8'))
                        for key, value in zip(['title', 'name', 'source_name'], row)
                        )
                    self.existing_packages_name.add(package_infos['name'])

    def retrieve_target(self):
        # Retrieve supplying organization (that will contain all harvested datasets).
        request = urllib2.Request(urlparse.urljoin(self.target_site_url,
            'api/3/action/organization_show?id={}'.format(self.supplier_name)), headers = self.target_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        supplier = conv.check(conv.pipe(
            conv.make_ckan_json_to_organization(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
        self.organization_by_name[self.supplier_name] = self.supplier = supplier
        self.retrieve_supplier_existing_packages(supplier)

        if self.old_supplier_name is not None:
            # Retrieve old supplying organization.
            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/organization_show?id={}'.format(self.old_supplier_name)), headers = self.target_headers)
            try:
                response = urllib2.urlopen(request)
            except urllib2.HTTPError as response:
                if response.code != 404:
                    raise
            else:
                response_dict = json.loads(response.read())
                old_supplier = conv.check(conv.pipe(
                    conv.make_ckan_json_to_organization(drop_none_values = True),
                    conv.not_none,
                    ))(response_dict['result'], state = conv.default_state)
                self.organization_by_name[self.old_supplier_name] = old_supplier
                self.retrieve_supplier_existing_packages(old_supplier)

    def update_target(self):
        # Upsert packages to target.
        for package_name, package in self.package_by_name.iteritems():
            log.info(u'Upserting package: {}'.format(package['title']))
            self.existing_packages_name.discard(package_name)
            self.upsert_package(package)

            # Read updated package.
            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/package_show?id={}'.format(package_name)), headers = self.target_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            package = conv.check(conv.pipe(
                conv.make_ckan_json_to_package(drop_none_values = True),
                conv.not_none,
                ))(response_dict['result'], state = conv.default_state)
            self.packages_by_organization_name.setdefault(self.organization_name_by_package_name[package_name],
                []).append(package)

            # Upsert package's related links.
            related = self.related_by_package_name.get(package_name)
            if related:
                # Retrieve package's related.
                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                    'api/3/action/related_list?id={}'.format(package_name)), headers = self.target_headers)
                response = urllib2.urlopen(request)
                response_dict = json.loads(response.read())
                existing_related = conv.check(conv.pipe(
                    conv.test_isinstance(list),
                    conv.uniform_sequence(
                        conv.make_ckan_json_to_related(drop_none_values = 'missing'),
                        drop_none_items = True,
                        ),
                    conv.empty_to_none,
                    ))(response_dict['result'], state = conv.default_state)
                for related_link in related:
                    related_link['dataset_id'] = package['id']
                    if related_link.get('description') is None:
                        # CKAN 2.1 displays "None" when description is missing.
                        related_link['description'] = u''
                    if related_link.get('url') is None:
                        # Weckan fails when url is missing.
                        related_link['url'] = u''
                    for existing_related_link in (existing_related or []):
                        if existing_related_link['title'] == related_link['title'] and (related_link.get('type') is None
                                or existing_related_link.get('type') == related_link['type']):
                            # Update related link.
                            if existing_related_link.get('description') != related_link.get('description') \
                                    or existing_related_link.get('image_url') != related_link.get('image_url') \
                                    or existing_related_link.get('url') != related_link.get('url'):
                                # Note: Currently, CKAN (2.1) doesn't accept that the owner of a related link updates it
                                # (even a sysadmin can't do it). So we delete it and recreate it.
#                                related_link['id'] = existing_related_link['id']
#                                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
#                                    'api/3/action/related_update?id={}'.format(related_link['id'])),
#                                    headers = self.target_headers)
#                                try:
#                                    response = urllib2.urlopen(request, urllib.quote(json.dumps(related_link)))
#                                except urllib2.HTTPError as response:
#                                    response_text = response.read()
#                                    log.error(u'An exception occured while updating related link: {0}'.format(
#                                        related_link))
#                                    try:
#                                        response_dict = json.loads(response_text)
#                                    except ValueError:
#                                        log.error(response_text)
#                                        raise
#                                    for key, value in response_dict.iteritems():
#                                        log.debug('{} = {}'.format(key, value))
#                                    raise
#                                else:
#                                    assert response.code == 200
#                                    response_dict = json.loads(response.read())
#                                    assert response_dict['success'] is True
##                                    updated_related_link = response_dict['result']
##                                    pprint.pprint(updated_related_link)
                                # Delete existing related link.
                                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                                    'api/3/action/related_delete?id={}'.format(existing_related_link['id'])),
                                    headers = self.target_headers)
                                try:
                                    response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_related_link)))
                                except urllib2.HTTPError as response:
                                    response_text = response.read()
                                    log.error(u'An exception occured while deleting related link: {0}'.format(
                                        existing_related_link))
                                    try:
                                        response_dict = json.loads(response_text)
                                    except ValueError:
                                        log.error(response_text)
                                        raise
                                    for key, value in response_dict.iteritems():
                                        log.debug('{} = {}'.format(key, value))
                                    raise
                                else:
                                    assert response.code == 200
                                    response_dict = json.loads(response.read())
                                    assert response_dict['success'] is True
                                # Recreate related link.
                                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                                    'api/3/action/related_create'), headers = self.target_headers)
                                try:
                                    response = urllib2.urlopen(request, urllib.quote(json.dumps(related_link)))
                                except urllib2.HTTPError as response:
                                    response_text = response.read()
                                    log.error(u'An exception occured while creating related link: {0}'.format(
                                        related_link))
                                    try:
                                        response_dict = json.loads(response_text)
                                    except ValueError:
                                        log.error(response_text)
                                        raise
                                    for key, value in response_dict.iteritems():
                                        log.debug('{} = {}'.format(key, value))
                                    raise
                                else:
                                    assert response.code == 200
                                    response_dict = json.loads(response.read())
                                    assert response_dict['success'] is True
#                                    created_related_link = response_dict['result']
#                                    pprint.pprint(created_related_link)
#                                    related_link['id'] = created_related_link['id']
                            break
                    else:
                        # Create related link.
                        request = urllib2.Request(urlparse.urljoin(self.target_site_url, 'api/3/action/related_create'),
                            headers = self.target_headers)
                        try:
                            response = urllib2.urlopen(request, urllib.quote(json.dumps(related_link)))
                        except urllib2.HTTPError as response:
                            response_text = response.read()
                            log.error(u'An exception occured while creating related link: {0}'.format(related_link))
                            try:
                                response_dict = json.loads(response_text)
                            except ValueError:
                                log.error(response_text)
                                raise
                            for key, value in response_dict.iteritems():
                                log.debug('{} = {}'.format(key, value))
                            raise
                        else:
                            assert response.code == 200
                            response_dict = json.loads(response.read())
                            assert response_dict['success'] is True
#                            created_related_link = response_dict['result']
#                            pprint.pprint(created_related_link)
#                            related_link['id'] = created_related_link['id']

        # Upsert lists of harvested packages into target.
        for organization_name, organization in self.organization_by_name.iteritems():
            package_title = u'Jeux de données - {}'.format(organization['title'])
            package_name = self.name_package(package_title)
            self.existing_packages_name.discard(package_name)
            packages = self.packages_by_organization_name.get(organization_name)
            if packages:
                log.info(u'Upserting package: {}'.format(package_name))
                packages_file = cStringIO.StringIO()
                packages_csv_writer = csv.writer(packages_file, delimiter = ';', quotechar = '"',
                    quoting = csv.QUOTE_MINIMAL)
                packages_csv_writer.writerow([
                    'Titre',
                    'Nom',
                    'Nom original',
                    'URL originale'
                    ])
                for package in packages:
                    package_source = self.package_source_by_name[package['name']]
                    packages_csv_writer.writerow([
                        package['title'].encode('utf-8'),
                        package['name'].encode('utf-8'),
                        package_source['name'].encode('utf-8'),
                        package_source['url'].encode('utf-8'),
                        ])
                file_metadata = filestores.upload_file(self.target_site_url, package_name,
                    packages_file.getvalue(), self.target_headers)

                package = dict(
                    author = self.supplier['title'],
                    license_id = 'fr-lo',
                    name = package_name,
                    notes = u'''Les jeux de données fournis par {} pour data.gouv.fr.'''.format(organization['title']),
                    owner_org = self.supplier['id'],
                    resources = [
                        dict(
                            created = file_metadata['_creation_date'],
                            format = 'CSV',
                            hash = file_metadata['_checksum'],
                            last_modified = file_metadata['_last_modified'],
                            name = package_name + u'.txt',
                            size = file_metadata['_content_length'],
                            url = file_metadata['_location'],
#                            revision_id – (optional)
#                            description (string) – (optional)
#                            resource_type (string) – (optional)
#                            mimetype (string) – (optional)
#                            mimetype_inner (string) – (optional)
#                            webstore_url (string) – (optional)
#                            cache_url (string) – (optional)
#                            cache_last_updated (iso date string) – (optional)
#                            webstore_last_updated (iso date string) – (optional)
                            ),
                        ],
                    tags = [
                        dict(
                            name = 'liste-de-jeux-de-donnees',
                            ),
                        ],
                    title = package_title,
                    )
                self.upsert_package(package)
            else:
                # Delete dataset if it exists.
                log.info(u'Deleting package: {}'.format(package_name))

                # Retrieve package id (needed for delete).
                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                    'api/3/action/package_show?id={}'.format(package_name)), headers = self.target_headers)
                try:
                    response = urllib2.urlopen(request)
                except urllib2.HTTPError as response:
                    if response.code != 404:
                        raise
                    # Package already deleted. Do nothing.
                    log.warning(u"Package to delete doesn't exist: {}".format(package_name))
                else:
                    response_dict = json.loads(response.read())
                    existing_package = response_dict['result']

                    # TODO: To replace with package_purge when it is available.
                    request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                        'api/3/action/package_delete?id={}'.format(package_name)), headers = self.target_headers)
                    response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
                    response_dict = json.loads(response.read())
#                    deleted_package = response_dict['result']
#                    pprint.pprint(deleted_package)

        # Delete obsolete packages.
        for package_name in self.existing_packages_name:
            # Retrieve package id (needed for delete).
            log.info(u'Deleting package: {}'.format(package_name))
            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/package_show?id={}'.format(package_name)), headers = self.target_headers)
            try:
                response = urllib2.urlopen(request)
            except urllib2.HTTPError as response:
                if response.code != 404:
                    raise
                # Package already deleted. Do nothing.
            else:
                response_dict = json.loads(response.read())
                existing_package = response_dict['result']

                request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                    'api/3/action/package_delete?id={}'.format(package_name)), headers = self.target_headers)
                response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
                response_dict = json.loads(response.read())
#                deleted_package = response_dict['result']
#                pprint.pprint(deleted_package)

    def upsert_group(self, group):
        name = strings.slugify(group['title'])[:100]

        existing_group = self.group_by_name.get(name)
        if existing_group is not None:
            return existing_group

        log.info(u'Upserting group: {}'.format(group['title']))
        if group.get('name') is None:
            group['name'] = name
        else:
            assert group['name'] == name, group

        request = urllib2.Request(urlparse.urljoin(self.target_site_url,
            'api/3/action/group_show?id={}'.format(name)), headers = self.target_headers)
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as response:
            if response.code != 404:
                raise
            existing_group = {}
        else:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while reading group: {0}'.format(name))
                log.error(response_text)
                raise
            existing_group = conv.check(conv.pipe(
                conv.make_ckan_json_to_group(drop_none_values = True),
                conv.not_none,
                ))(response_dict['result'], state = conv.default_state)

            group_infos = group
            group = conv.check(conv.ckan_input_group_to_output_group)(existing_group, state = conv.default_state)
            group.update(
                (key, value)
                for key, value in group_infos.iteritems()
                if value is not None
                )

        if existing_group.get('id') is None:
            # Create group.
            request = urllib2.Request(urlparse.urljoin(self.target_site_url, 'api/3/action/group_create'),
                headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(group)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while creating group: {0}'.format(group))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
                created_group = response_dict['result']
#                pprint.pprint(created_group)
                group['id'] = created_group['id']
        else:
            # Update group.
            group['id'] = existing_group['id']
            group['state'] = 'active'

            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/group_update?id={}'.format(name)), headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(group)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while updating group: {0}'.format(group))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                updated_group = response_dict['result']
#                pprint.pprint(updated_group)

        self.group_by_name[name] = group
        return group

    def upsert_organization(self, organization):
        name = strings.slugify(organization['title'])[:100]

        existing_organization = self.organization_by_name.get(name)
        if existing_organization is not None:
            return existing_organization

        log.info(u'Upserting organization: {}'.format(organization['title']))
        if organization.get('name') is None:
            organization['name'] = name
        else:
            assert organization['name'] == name, organization

        request = urllib2.Request(urlparse.urljoin(self.target_site_url,
            'api/3/action/organization_show?id={}'.format(name)), headers = self.target_headers)
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as response:
            if response.code != 404:
                raise
            existing_organization = {}
        else:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while reading organization: {0}'.format(name))
                log.error(response_text)
                raise
            existing_organization = conv.check(conv.pipe(
                conv.make_ckan_json_to_organization(drop_none_values = True),
                conv.not_none,
                ))(response_dict['result'], state = conv.default_state)

            organization_infos = organization
            organization = conv.check(conv.ckan_input_organization_to_output_organization)(existing_organization,
                state = conv.default_state)
            organization.update(
                (key, value)
                for key, value in organization_infos.iteritems()
                if value is not None
                )

        if existing_organization.get('id') is None:
            # Create organization.
            request = urllib2.Request(urlparse.urljoin(self.target_site_url, 'api/3/action/organization_create'),
                headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while creating organization: {0}'.format(organization))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
                created_organization = response_dict['result']
#                pprint.pprint(created_organization)
                organization['id'] = created_organization['id']
        else:
            # Update organization.
            organization['id'] = existing_organization['id']
            organization['state'] = 'active'

            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/organization_update?id={}'.format(name)), headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while updating organization: {0}'.format(organization))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                updated_organization = response_dict['result']
#                pprint.pprint(updated_organization)

        self.organization_by_name[name] = organization
        return organization

    def upsert_package(self, package):
        name = package.get('name')
        assert name is not None, package

        request = urllib2.Request(urlparse.urljoin(self.target_site_url,
            'api/3/action/package_show?id={}'.format(name)), headers = self.target_headers)
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
            request = urllib2.Request(urlparse.urljoin(self.target_site_url, 'api/3/action/package_create'),
                headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while creating package: {0}'.format(package))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
                created_package = response_dict['result']
#                pprint.pprint(created_package)
                package['id'] = created_package['id']
        else:
            # Update package.
            package['id'] = existing_package['id']
            package['state'] = 'active'

            # Keep existing groups when they already exist.
            existing_groups = [
                dict(id = existing_group['id'])
                for existing_group in (existing_package.get('groups') or [])
                ]
            if existing_groups:
                if package.get('groups'):
                    groups = package['groups']
                    for existing_group in existing_groups:
                        if not any(
                                group['id'] == existing_group['id']
                                for group in groups
                                ):
                            groups.append(existing_group)
                else:
                    package['groups'] = existing_groups

            request = urllib2.Request(urlparse.urljoin(self.target_site_url,
                'api/3/action/package_update?id={}'.format(name)), headers = self.target_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while updating package: {0}'.format(package))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    log.debug('{} = {}'.format(key, value))
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                updated_package = response_dict['result']
#                pprint.pprint(updated_package)
        return package


def get_extra(instance, key, default = UnboundLocalError):
    for extra in (instance.get('extras') or []):
        if extra['key'] == key:
            return extra.get('value')
    if default is UnboundLocalError:
        raise KeyError(key)
    return default


def pop_extra(instance, key, default = UnboundLocalError):
    for index, extra in enumerate(instance.get('extras') or []):
        if extra['key'] == key:
            del instance['extras'][index]
            return extra.get('value')
    if default is UnboundLocalError:
        raise KeyError(key)
    return default


def set_extra(instance, key, value):
    if value is None:
        pop_extra(instance, key, default = None)
        return
    if instance.get('extras') is None:
        instance['extras'] = []
    for extra in instance['extras']:
        if extra['key'] == key:
            extra['value'] = value
            return
    instance['extras'].append(dict(
        key = key,
        value = value,
        ))
