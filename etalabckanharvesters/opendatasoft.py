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


"""Harvest Open Data Soft CKAN repository.

http://www.opendatasoft.com/
"""


import argparse
import base64
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
                'opendatasoft.ckan.password': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'opendatasoft.ckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'opendatasoft.ckan.username': conv.pipe(
                    conv.cleanup_line,
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
        ))(dict(config_parser.items('Etalab-OpenDataSoft-Harvester')), conv.default_state)

    organization_by_name = {}
    source_headers = {
        'Authorization': 'Basic {}'.format(base64.encodestring('{}:{}'.format(conf['opendatasoft.ckan.username'],
            conf['opendatasoft.ckan.password'])).replace('\n', '')),
        'User-Agent': conf['user_agent'],
        }
    source_site_url = conf['opendatasoft.ckan.site_url']
    supplier_name = u'opendatasoft'
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

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_list'),
        headers = source_headers)
    response = urllib2.urlopen(request)
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
            headers = source_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                id = package_source_name,
                ))))  # CKAN 1.7 requires a POST.
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            before_ckan_json_to_package,
            conv.make_ckan_json_to_package(drop_none_values = True),
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if package is None:
            continue

        publisher = helpers.get_extra(package, 'publisher')
        organization_infos = {
            None: (u"OpenDataSoft", None),
            u"adt-et-ots-des-alpes-de-haute-provence": False,  # Datasets must be merged.
            u"agence-bio": False,  # Direct member of data.gouv.fr
            u"agence-des-espaces-verts-idf": (None, None),
            u"autolib": (None, None),
            u"comite-departemental-de-tourisme-du-pas-de-calais": False,
            u"conseil-general-des-hauts-de-seine": (None, None),
            u"ctc-corse": False,  # Bad titles and descriptions
            u"direction-regionale-du-travail-de-l-emploi-et-de-la-formation-professionnelle": False,  # Direct member of data.gouv.fr?
            u"driea-sit-del-2": (None, None),
            u"federation-nationale-des-bistrots-de-pays": (None, None),
            u"gip-corse-competences": (u"GIP Corse Compétences", None),
            u"iau-idf": (None, None),
            u"ign": False,  # Direct member of data.gouv.fr
            u"insee": False,  # Direct member of data.gouv.fr
            u"jcdecaux-developer": (None, None),
            u"la-poste": False,  # Direct member of data.gouv.fr
            u"le-rif": (None, None),
            u"ministere-de-l-education-nationale": False,  # Direct member of data.gouv.fr
            u"ministere-de-l-interieur": False,  # Direct member of data.gouv.fr
            u"ministere-de-la-culture-et-de-la-communication": False,  # Direct member of data.gouv.fr
            u"ministere-de-la-justice": False,  # Direct member of data.gouv.fr
            u"ministere-des-sports": False,  # Direct member of data.gouv.fr
            u"premier-ministre-direction-de-l-information-legale-et-administrative": False,  # Direct member of data.gouv.fr
            u"ratp": False,  # Direct member of data.gouv.fr
            u"reseau-ferre-de-france": False,  # Direct member of data.gouv.fr
            u"region-ile-de-france": False,  # Datasets must be merged.
            u"sncf": (u"Société nationale des chemins de fer français", None),  # Direct member of data.gouv.fr, but other datasets
            u"societe-nationale-des-chemins-de-fer-francais": False,  # Direct member of data.gouv.fr
            u"ville-de-paris": (u"Mairie de Paris", None),
            u"ville-de-paris-direction-de-la-proprete-et-de-l-eau": (u"Mairie de Paris",
                u"Direction de la propreté et de l'eau"),
            }.get(strings.slugify(publisher))
        if organization_infos is None:
            log.warning(u'Ignoring package "{}" from unknown publisher "{}"'.format(package['title'], publisher))
            continue
        if organization_infos is False:
            continue
        organization_title, author = organization_infos
        if organization_title is None:
            organization_title = publisher
        organization_name = strings.slugify(organization_title)[:100]
        organization = organization_by_name.get(organization_name)
        if organization is None:
            log.info(u'Upserting organization: {}'.format(organization_title))
            organization = helpers.upsert_organization(target_site_url, dict(
                name = organization_name,
                title = organization_title,
                ), headers = target_headers)
            organization_by_name[organization_name] = organization

        package['author'] = author
        package.pop('groups', None)
        del package['id']

        package_name = strings.slugify(package['title'])[:100]
        package_source_name_by_name[package_name] = package['name']
        package['name'] = package_name
        organization_name_by_package_name[package_name] = organization['name']

        package['owner_org'] = organization['id']
        package.pop('users', None)

        helpers.set_extra(package, 'harvest_app_name', app_name)
        helpers.pop_extra(package, 'publisher', None)
        helpers.set_extra(package, 'supplier_id', supplier['id'])

        package = conv.check(conv.ckan_input_package_to_output_package)(package, state = conv.default_state)
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
                    package_source_name_by_name[organization_package['name']].encode('utf-8'),
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
