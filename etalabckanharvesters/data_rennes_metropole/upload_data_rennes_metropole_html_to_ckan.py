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


import argparse
import ConfigParser
import logging
import os
import re
import sys
import urlparse

from biryani1 import baseconv, custom_conv, states
from lxml import etree

from .. import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, states)
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

    harvester = helpers.Harvester(
        supplier_abbreviation = u'rm',
        supplier_title = u'Rennes Métropole',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )

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

    harvester.retrieve_target()

    # Convert source HTML packages to CKAN JSON.
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
                organization = harvester.upsert_organization(dict(
                    title = organization_title,
                    ))

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
                    './/div[@class="tx_icsopendatastore_pi1_technical_data separator"]'
                    '/p[@class="value technical_data"]')
                technical_data_str = technical_data_html_list[0].text.strip() or None if technical_data_html_list \
                    else None

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
            notes = description_str,
            resources = resources,
            tags = tags,
            territorial_coverage = u'IntercommunalityOfFrance/243500139',  # Rennes-Métropole, TODO
            title = title_str,
            )
        helpers.set_extra(package, u'Données techniques', technical_data_str)
        helpers.set_extra(package, u'Auteur', creator_str)
        helpers.set_extra(package, u'Propriétaire', owner_str)
        helpers.set_extra(package, u'Date de mise à disposition', release_date_str)
        helpers.set_extra(package, u'Date de mise à jour', update_date_str)
        source_url = u'http://www.data.rennes-metropole.fr/les-donnees/catalogue/?tx_icsopendatastore_pi1[uid]={}' \
            .format(data_number)
        helpers.set_extra(package, u'Source', source_url)

        harvester.add_package(package, organization, package['title'], source_url)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
