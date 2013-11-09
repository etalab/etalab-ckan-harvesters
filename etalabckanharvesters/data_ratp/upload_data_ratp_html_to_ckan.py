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
data_filename_re = re.compile('(?P<name>[-0-9a-z]+)\.html$')
french_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]?\d|3[01])/(?P<month>0?[1-9]|1[0-2])/(?P<year>[12]\d\d\d)')
html_parser = etree.HTMLParser()
log = logging.getLogger(app_name)
trimester_re = re.compile(ur'T(?P<trimester>[1-4]) (?P<year>\d{4})$')
year_re = re.compile(ur'Année (?P<year>\d{4})$')


def french_input_to_date(value, state = None):
    if value is None:
        return value, None
    match = french_date_re.match(value)
    if match is None:
        return value, (state or conv.default_state)._(u"Invalid french date")
    return datetime.date(int(match.group('year')), int(match.group('month')), int(match.group('day'))), None


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
        supplier_abbreviation = u'ratp',
        supplier_title = u'Régie autonome des transports parisiens (RATP)',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )

    # Retrieve paths of HTML pages to convert.
    data_dir = os.path.join(args.download_dir, 'data')
    assert os.path.exists(data_dir), "Data directory {0} doesn't exist".format(data_dir)
    data_file_path_by_name = {}
    for (dir, directories_name, filenames) in os.walk(data_dir):
        for directory_name in directories_name[:]:
            if directory_name.startswith('.'):
                directories_name.remove(directory_name)
        for filename in filenames:
            data_file_path = os.path.join(dir, filename)
            match = data_filename_re.match(os.path.basename(data_file_path))
            assert match is not None, data_file_path
            data_file_path_by_name[match.group('name')] = data_file_path

    harvester.retrieve_target()

    # Convert source HTML packages to CKAN JSON.
    for data_name, data_file_path in sorted(data_file_path_by_name.iteritems()):
        with open(data_file_path) as data_file:
            try:
                data_str = data_file.read()
                data_html = etree.fromstring(data_str, html_parser)
                html_base_list = data_html.xpath('head/base[@href]')
                base_url = html_base_list[0].get('href')

                dataset_html = data_html.xpath('.//div[@class="tx_icsoddatastore_pi1_single"]')[0]
                assert dataset_html is not None

                title_str = dataset_html.xpath('.//h1')[0].text.strip()
                assert title_str

                description_str = dataset_html.xpath('.//p[@class="value description"]')[0].text.strip()
                assert description_str
                description_str = description_str.replace(u'<br />', u'\n\n')

                license_url = dataset_html.xpath('.//h4[starts-with(., "Licence :")]//a')[0].get('href').strip()
                assert license_url
                license_id = {
                    'http://opendatacommons.org/licenses/odbl/1.0/': 'odc-odbl',
                    'https://www.data.gouv.fr/Licence-Ouverte-Open-Licence': 'fr-lo',
                    'fileadmin/Documents/conditions_generales_dutilisation_0213.pdf': 'other-closed',  # RATP
                    }[license_url]

                categories_html_list = dataset_html.xpath('.//span[@class="categorie"]')
                tags = [
                    dict(name = strings.slugify(category_html.text.strip()))
                    for category_html in categories_html_list
                    ]
                assert tags

                resources = []
                resources_sections_list = dataset_html.xpath(u'.//div[@class="section_file"]')
                for data_index, a_html in enumerate(resources_sections_list[0].xpath('.//a')):
                    format = {
                        u'CSV': u'CSV',
                        u'PDF': u'PDF',
                        u'XLS': u'XLS',
                        u'ZIP': u'ZIP',
                        u'Other': u'autre',
                        }.get(a_html.text)
                    assert format is not None, a_html.text
                    resources.append(dict(
                        format = format,
                        name = u'Données' if data_index == 0 else u'Données {}'.format(data_index + 1),
                        url = urlparse.urljoin(base_url, a_html.get('href')),
                        ))
                if len(resources_sections_list) > 1:
                    for data_index, a_html in enumerate(resources_sections_list[1].xpath('.//a')):
                        format = {
                            u'PDF': u'PDF',
                            }.get(a_html.text)
                        assert format is not None, a_html.text
                        resources.append(dict(
                            format = format,
                            name = u'Document complémentaire' if data_index == 0
                                else u'Document complémentaire {}'.format(data_index + 1),
                            url = urlparse.urljoin(base_url, a_html.get('href')),
                            ))

                fields = {}
                for li_html in dataset_html.xpath('.//ul/li[span/@class="label"]'):
                    label_html, value_html = li_html.xpath('span')
                    fields[label_html.text.strip()] = value_html.text
                editor = fields.pop(u'Editeur :')
                assert editor is None, editor
                owner = fields.pop(u'Propriétaire :')
                assert owner == u'RATP', owner
                organization = harvester.supplier
                author = fields.pop(u'Gestionnaire :')
                assert author in (
                    u'Département Commercial',
                    u'Département Communication',
                    u'Département Développement, Innovation et Territoires',
                    ), author
                contact = fields.pop(u'Contact :')
                assert contact == u'Equipe OpenData RATP', contact
                publication_date_str = fields.pop(u'Date de publication :')
                publication_date_iso8601_str = conv.check(conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    ))(publication_date_str, state = conv.default_state)
                update_date_str = fields.pop(u'Date de mise à jour :')
                update_date_iso8601_str = conv.check(conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    ))(update_date_str, state = conv.default_state)
                validity_period = fields.pop(u'Période de validité :')
                if validity_period in (None, u'Période de validité'):
                    temporal_coverage_from = None
                    temporal_coverage_to = None
                else:
                    match = trimester_re.match(validity_period)
                    if match is None:
                        match = year_re.match(validity_period)
                        assert match is not None, str((validity_period,))
                        temporal_coverage_from = temporal_coverage_to = match.group('year')
                    else:
                        trimester = int(match.group('trimester'))
                        temporal_coverage_from = u'{}-{:02d}'.format(match.group('year'), (trimester - 1) * 3 + 1)
                        temporal_coverage_to = u'{}-{:02d}'.format(match.group('year'), (trimester - 1) * 3 + 3)
                update_frequency = fields.pop(u'Fréquence de mise à jour :')
                frequency = {
                    None: None,
                    u'Annuelle': u'annuelle',
                    u'Trimestrielle': u'trimestrielle',
                    u'Variable': u'ponctuelle',
                    }.get(update_frequency, UnboundLocalError)
                assert frequency is not UnboundLocalError, update_frequency

                assert not fields, fields
            except:
                print 'An exception occured in file {0}'.format(data_name)
                raise

        package = dict(
            author = author,
            frequency = frequency,
            license_id = license_id,
            notes = description_str,
            resources = resources,
            tags = tags,
            temporal_coverage_from = temporal_coverage_from,
            temporal_coverage_to = temporal_coverage_to,
            territorial_coverage = u'CommuneOfFrance/75056',
            territorial_coverage_granularity = 'poi',
            title = title_str,
            url = u'http://data.ratp.fr/fr/les-donnees/fiche-de-jeu-de-donnees/dataset/{}'.format(data_name),
            )
#        helpers.set_extra(package, u'Données techniques', technical_data_str)
#        helpers.set_extra(package, u'Auteur', creator_str)
#        helpers.set_extra(package, u'Propriétaire', owner_str)
        helpers.set_extra(package, u'Date de publication', publication_date_iso8601_str)
        helpers.set_extra(package, u'Date de mise à jour', update_date_iso8601_str)

        harvester.add_package(package, organization, package['title'], package['url'])

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
