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
import datetime
import logging
import os
import re
import sys
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, states, strings
from lxml import etree

from .. import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, states)
data_filename_re = re.compile('data-(?P<number>\d+)\.html$')
french_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]\d|3[01]) (?P<month>.+) (?P<year>[12]\d\d\d)')
french_numeric_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]\d|3[01])/(?P<month>0?[1-9]|1[0-2])/(?P<year>[12]\d\d\d)')
frequency_translations = {
    u"annuelle": u"annuelle",
    u"mensuelle": u"mensuelle",
    u"Quotidienne": u"quotidienne",
    }
html_parser = etree.HTMLParser()
license_id_by_str = {
    u"Licence infolocale": u'other-open',
    u"Licence Rennes Métropole V2": u'other-at',
    u"Open Database License (ODbL)": u'odc-odbl',
    }
log = logging.getLogger(app_name)
organization_titles_by_owner_str = {
    u"Arts vivants en Ille-et-Vilaine": (u"Arts vivants en Ille-et-Vilaine", None),
    u"Association Trans Musicales": (u"Association Trans Musicales", None),
    u"Direction des Affaires Financières": (u"Rennes Métropole", u"Direction des Affaires Financières"),
    u"Infocolale/Ouest-France": (u"Ouest France", u"Infolocale.fr"),
    u"Keolis Rennes": (u"Keolis", u"Keolis Rennes"),
    u"Rennes Métropole": (u"Rennes Métropole", None),
    u"Service SIG Rennes Métropole": (u"Rennes Métropole", u"Service SIG Rennes Métropole"),
    u"Ville de Rennes": (u"Ville de Rennes", None),
    }


def french_input_to_date(value, state = None):
    if value is None:
        return value, None
    match = french_numeric_date_re.match(value)
    if match is None:
        match = french_date_re.match(value)
        if match is None:
            return value, (state or conv.default_state)._(u"Invalid french date")
        return datetime.date(
            int(match.group('year')),
            {
                u'août': 8,
                u'avril': 4,
                u'décembre': 12,
                u'février': 2,
                u'janvier': 1,
                u'juin': 6,
                u'juillet': 7,
                u'mai': 5,
                u'mars': 3,
                u'novembre': 11,
                u'octobre': 10,
                u'septembre': 9,
                }[match.group('month')],
            int(match.group('day')),
            ), None
    return datetime.date(
        int(match.group('year')),
        int(match.group('month')),
        int(match.group('day')),
        ), None


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('download_dir', help = 'directory where are stored downloaded HTML pages')
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
        admin_name = u'b-dot-kessler-at-agglo-rennesmetropole-dot-fr',
        supplier_abbreviation = u'rm',
        supplier_title = u'Rennes Métropole en accès libre',
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

    if not args.dry_run:
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

                contact_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_contact separator"]/p[@class="value description"]')
                contact_str = contact_html_list[0].text.strip() or None if contact_html_list else None

                creator_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_creator separator"]/p[@class="value description"]')
                creator_str = creator_html_list[0].text.strip() or None if creator_html_list else None

                owner_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_owner separator"]/p[@class="value owner"]')
                owner_str = owner_html_list[0].text.strip() or None if owner_html_list else None
                organization_title, author = conv.check(conv.pipe(
                    conv.test_in(organization_titles_by_owner_str),
                    conv.translate(organization_titles_by_owner_str),
                    conv.default((u"Rennes Métropole", None)),
                    ))(owner_str, state = conv.default_state)
                if not args.dry_run:
                    organization = harvester.upsert_organization(dict(
                        title = organization_title,
                        ))

                categories_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_categories separator"]/p[@class="value description"]')
                categories_str = categories_html_list[0].text.strip() or None if categories_html_list else None
                tags = [
                    dict(name = tag_name)
                    for tag_name in sorted(set(
                        strings.slugify(category_fragment)
                        for category_str in categories_str.split(u',')
                        for category_fragment in category_str.split(u':')
                        ))
                    ]
                if not args.dry_run:
                    groups = [
                        harvester.upsert_group(dict(
                            title = categories_str.split(u',')[0].strip(),
                            )),
                        harvester.upsert_group(dict(
                            title = u'Territoires et Transports',
                            )),
                        ] if categories_str else None

                release_date_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_releasedate separator"]/p[@class="value description"]')
                release_date_str = release_date_html_list[0].text if release_date_html_list else None
                release_date_iso8601_str = conv.check(conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    ))(release_date_str, state = conv.default_state)

                update_date_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_updatedate separator"]/p[@class="value description"]')
                update_date_str = update_date_html_list[0].text if update_date_html_list else None
                update_date_iso8601_str = conv.check(conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    ))(update_date_str, state = conv.default_state)

                frequency_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_updatefrequency separator"]/p[@class="value description"]')
                frequency_str = frequency_html_list[0].text if frequency_html_list else None
                frequency = conv.check(conv.pipe(
                    conv.cleanup_line,
                    conv.test_in(frequency_translations),
                    conv.translate(frequency_translations),
                    ))(frequency_str, state = conv.default_state)

                description_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_description separator"]/p[@class="value description"]')
                description_str = description_html_list[0].text.strip() or None if description_html_list else None

                technical_data_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_technical_data separator"]'
                    '/p[@class="value technical_data"]')
                technical_data_str = technical_data_html_list[0].text.strip() or None if technical_data_html_list \
                    else None

                license_html_list = dataset_html.xpath(
                    './/div[@class="tx_icsopendatastore_pi1_licence separator"]/p[@class="value owner"]/a')
                license_str = license_html_list[0].text if license_html_list else None
                license_id = conv.check(conv.pipe(
                    conv.cleanup_line,
                    conv.test_in(license_id_by_str),
                    conv.translate(license_id_by_str),
                    ))(license_str, state = conv.default_state)

                resources = []
                for resource_html in dataset_html.xpath('.//div[@class="tx_icsopendatastore_pi1_file"]'):
                    resource_url = urlparse.urljoin(base_url, resource_html.xpath('.//a[@href]')[0].get('href'))
                    resource_path = urlparse.urlsplit(resource_url)
                    filename = resource_url.rstrip('/').rsplit(u'/', 1)[-1] or u'Fichier'
                    if not filename or fi
                    resources.append(dict(
                        created = release_date_iso8601_str,
                        format = resource_html.xpath('.//span[@class="coin"]')[0].text.strip() or None,
                        last_modified = update_date_iso8601_str,
                        name = filename,
                        url = resource_url,
                        ))
            except:
                print 'An exception occured in file {0}'.format(data_number)
                raise

        package = dict(
            author = author,
            frequency = frequency,
            license_id = license_id,
            maintainer = contact_str,
            notes = description_str,
            resources = resources,
            tags = tags,
            territorial_coverage = u'IntercommunalityOfFrance/243500139/CA RENNES METROPOLE',
            title = title_str,
            url = u'http://www.data.rennes-metropole.fr/les-donnees/catalogue/?tx_icsopendatastore_pi1[uid]={}'
                .format(data_number),
            )
        helpers.set_extra(package, u'Données techniques', technical_data_str)
        helpers.set_extra(package, u'Éditeur', publisher_str)
        helpers.set_extra(package, u'Auteur', creator_str)

        if not args.dry_run:
            harvester.add_package(package, organization, package['title'], package['url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
