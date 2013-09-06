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


"""Harvest Lutèce from City of Paris

http://dev.lutece.paris.fr/plugins/module-document-ckan/
"""


import argparse
import ConfigParser
import cStringIO
import csv
import json
import logging
import os
import re
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv, filestores
from lxml import etree

from .. import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
data_filename_re = re.compile('data-(?P<number>\d+)\.html$')
html_parser = etree.HTMLParser()
log = logging.getLogger(app_name)


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('download_dir', help = 'directory where are stored downloaded HTML pages')
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

    target_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }
    organization_by_name = {}
    supplier_name = u'rennes-metropole'
    target_site_url = conf['ckan.site_url']

    # Retrieve paths of HTML pages to convert.
    data_dir = os.path.join(args.download_dir, 'data')
    assert os.path.exists(data_dir), "Data directory {0} doesn't exist".format(data_dir)
    data_file_path_by_number = {}
    for (dir, directories_name, filenames) in os.walk(data_dir):
        for directory_name in directories_name[:]:
            if directory_name.startswith('.'):
                directories_name.remove(directory_name)
        for filename in filenames:
            data_file_path = os.path.join(dir, filename)
            match = data_filename_re.match(os.path.basename(data_file_path))
            assert match is not None, data_file_path
            data_number = int(match.group('number'))
            data_file_path_by_number[data_number] = data_file_path

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

    supplier_package_title = u'Jeux de données - {}'.format(supplier['title'])
    supplier_package_name = strings.slugify(supplier_package_title)[:100]

    existing_packages_name = set()
    if supplier_package_name in (
            package['name']
            for package in (supplier.get('packages') or [])
            ):
        request = urllib2.Request(urlparse.urljoin(target_site_url,
            'api/3/action/package_show?id={}'.format(supplier_package_name)), headers = target_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        supplier_package = conv.check(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            )(response_dict['result'], state = conv.default_state)
        for tag in (supplier_package.get('tags') or []):
            if tag['name'] == 'liste-de-jeux-de-donnees':
                existing_packages_name.add(supplier_package['name'])
                for resource in (supplier_package.get('resources') or []):
                    response = urllib2.urlopen(resource['url'])
                    packages_csv_reader = csv.reader(response, delimiter = ';', quotechar = '"')
                    packages_csv_reader.next()
                    for row in packages_csv_reader:
                        package_infos = dict(
                            (key, value.decode('utf-8'))
                            for key, value in zip(['title', 'name', 'source_name'], row)
                            )
                        existing_packages_name.add(package_infos['name'])
                break
        else:
            # This dataset doesn't contain a list of datasets. Ignore it.
            pass

    # Convert source HTML packages to CKAN JSON.
    organization_name_by_package_name = {}
    package_by_name = {}
    for data_number, data_file_path in sorted(data_file_path_by_number.iteritems()):
        with open(data_file_path) as data_file:
            try:
                data_str = data_file.read()
                data_html = etree.fromstring(data_str, html_parser)
                html_base_list = data_html.xpath('head/base[@href]')
                base_url = html_base_list[0].get('href')

                dataset_html = data_html.xpath('.//div[@class="tx_icsopendatastore_pi1_single"]')[0]
                assert dataset_html is not None
                title_str = dataset_html.xpath('.//h3')[0].text.strip()
                assert title_str

                publisher_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_publisher separator"]/p[@class="value description"]')
                publisher_str = publisher_html_list[0].text.strip() or None if publisher_html_list else None
                organization_title, author = {
                    None: (u"Rennes Métropole", None),
                    u"Direction des Affaires Financières": (u"Rennes Métropole", u"Direction des Affaires Financières"),
                    u"Keolis Rennes": (u"Keolis", u"Keolis Rennes"),
                    u"Rennes Métropole": (u"Rennes Métropole", None),
                    u"Service DPAP Ville  de Rennes": (u"Ville de Rennes", u"Service DPAP"),
                    u"Service SIG Rennes Métropole": (u"Rennes Métropole", u"Service SIG Rennes Métropole"),
                    u"Ville de Rennes": (u"Ville de Rennes", None),
                    }.get(publisher_str, (u"Rennes Métropole", publisher_str))
                organization_name = strings.slugify(organization_title)[:100]
                organization = organization_by_name.get(organization_name)
                if organization is None:
                    log.info(u'Upserting organization: {}'.format(organization_title))
                    organization = helpers.upsert_organization(target_site_url, dict(
                        name = organization_name,
                        title = organization_title,
                        ), headers = target_headers)
                    organization_by_name[organization_name] = organization

                contact_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_contact separator"]/p[@class="value description"]')
                contact_str = contact_html_list[0].text.strip() or None if contact_html_list else None

                creator_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_creator separator"]/p[@class="value description"]')
                creator_str = creator_html_list[0].text.strip() or None if creator_html_list else None

                owner_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_owner separator"]/p[@class="value description"]')
                owner_str = owner_html_list[0].text.strip() or None if owner_html_list else None

                categories_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_categories separator"]/p[@class="value description"]')
                categories_str = categories_html_list[0].text.strip() or None if categories_html_list else None
                tags = [
                    dict(name = category_str)
                    for category_str in categories_str.split(u', ')
                    ]

                release_date_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_releasedate separator"]/p[@class="value description"]')
                release_date_str = release_date_html_list[0].text.strip() or None if release_date_html_list else None
                # TODO: Convert french date format to ISO.

                update_date_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_updatedate separator"]/p[@class="value description"]')
                update_date_str = update_date_html_list[0].text.strip() or None if update_date_html_list else None
                # TODO: Convert french date format to ISO.

                description_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_description separator"]/p[@class="value description"]')
                description_str = description_html_list[0].text.strip() or None if description_html_list else None

                technical_data_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_technical_data separator"]/p[@class="value technical_data"]')
                technical_data_str = technical_data_html_list[0].text.strip() or None if technical_data_html_list else None

                resources = [
                    dict(
                        format = resource_html.xpath('.//span[@class="coin"]')[0].text.strip() or None,
                        url = urlparse.urljoin(base_url, resource_html.xpath('.//a[@href]')[0].get('href')),
                        )
                    for resource_html in dataset_html.xpath('.//div[@class="tx_icsopendatastore_pi1_file"]')
                    ]
            except:
                print 'An exception occured in file {0}'.format(data_number)
                raise

        package = dict(
            author = author,
            maintainer = contact_str,
            name = strings.slugify(title_str)[:100],
            notes = description_str,
            owner_org = organization['id'],
            resources = resources,
            supplier_id = supplier['id'],
            tags = tags,
            territorial_coverage = u'IntercommunalityOfFrance/243500139',  # Rennes-Métropole, TODO
            title = title_str,
            )
        helpers.set_extra(package, u'Données techniques', technical_data_str)
        helpers.set_extra(package, u'Auteur', creator_str)
        helpers.set_extra(package, u'Propriétaire', owner_str)
        helpers.set_extra(package, u'Date de mise à disposition', release_date_str)
        helpers.set_extra(package, u'Date de mise à jour', update_date_str)
        helpers.set_extra(package, 'harvest_app_name', app_name)

        package_by_name[package['name']] = package
        organization_name_by_package_name[package['name']] = organization['name']

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
                    organization_package['name'].encode('utf-8'),
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
