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


"""Harvest open data from "Toulouse Métropole.data".

http://data.toulouse-metropole.fr/
"""


import argparse
import ConfigParser
import cStringIO
import csv
import datetime
import logging
import os
import re
import sys
import urllib2
import urlparse
import zipfile

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, datetimeconv, jsonconv, states)
french_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]\d|3[01])/(?P<month>0?[1-9]|1[0-2])/(?P<year>[12]\d\d\d)')
frequency_by_accrualPeriodicity = {
    u'Annuelle': u"annuelle",
    u'API': u"temps réel",
    u'Bihebdomadaire': u"hebdomadaire",  # TODO
    u'Bimensuelle': u"bimensuelle",
    u'Hebdomadaire': u"hebdomadaire",
    u'Production unique': u"aucune",
    u'Quotidienne': u"quotidienne",
    u'sans objet': u"aucune",
    }
license_id_by_licence = {
    u'licence-ouverte-open-license': u'fr-lo',
    u'odbl': u'odc-odbl',
    }
log = logging.getLogger(app_name)
N_ = lambda message: message
organization_title_translations = {
#    u"JC Decaux",
#    u"La Poste",
#    u"Mairie de Balma",
#    u"Mairie de Seilh",
#    u"Mairie de Toulouse",
#    u"Office de tourisme So Toulouse",
#    u"Tisseo SMTC",
#    u"Toulouse métrople",
    }
organizations_title_to_ignore = set([
    u"JC Decaux",
    u"La Poste",
    ])
territory_error_couple_by_name = {}


# Converters


def french_input_to_date(value, state = None):
    if value is None:
        return value, None
    match = french_date_re.match(value)
    if match is None:
        return value, (state or conv.default_state)._(u"Invalid french date")
    return datetime.date(
        int(match.group('year')),
        int(match.group('month')),
        int(match.group('day')),
        ), None


def str_to_territory(territory_name, state = None):
    if territory_name is None:
        return territory_name, None
    territory_error_couple = territory_error_couple_by_name.get(territory_name)
    if territory_error_couple is None:
        if state is None:
            state = conv.default_state
        url = urlparse.urljoin(conf['territoria.site_url'], u'api/v1/autocomplete-territory/'
            u'?kind=CommuneOfFrance&parent=intercommunalites/cu-du-grand-toulouse&term={}'.format(territory_name))
        request = urllib2.Request(url)
        response = urllib2.urlopen(request)
        territory_error_couple_by_name[territory_name] = territory_error_couple = conv.pipe(
            conv.make_input_to_json(),
            conv.test_isinstance(dict),
            conv.struct(
                dict(
                    apiVersion = conv.pipe(
                        conv.test_isinstance(basestring),
                        conv.test_equals(u'1.0'),
                        conv.not_none,
                        ),
                    data = conv.pipe(
                        conv.test_isinstance(dict),
                        conv.struct(
                            dict(
                                items = conv.pipe(
                                    conv.test_isinstance(list),
                                    conv.test(lambda value: len(value) >= 1, error = N_(u'Unknown territory')),
                                    conv.uniform_sequence(
                                        conv.pipe(
                                            conv.test_isinstance(dict),
                                            conv.struct(
                                                dict(
                                                    code = conv.pipe(
                                                        conv.test_isinstance(basestring),
                                                        conv.not_none,
                                                        ),
                                                    kind = conv.pipe(
                                                        conv.test_isinstance(basestring),
                                                        conv.not_none,
                                                        ),
                                                    main_postal_distribution = conv.pipe(
                                                        conv.test_isinstance(basestring),
                                                        conv.not_none,
                                                        ),
                                                    ),
                                                default = conv.noop,
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            default = conv.noop,
                            ),
                        ),
                    ),
                default = conv.noop,
                ),
            conv.function(lambda value: value['data']['items'][0]),
            conv.not_none,
            )(response.read(), state = state)
    return territory_error_couple


entry_to_dataset = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        {
            u'dcat:accessUrl': conv.pipe(
                conv.make_input_to_url(full = True),
                conv.not_none,
                ),
            u'dcat:dataset': conv.pipe(
                conv.make_input_to_url(full = True),
                conv.not_none,
                ),
            u'dcat:distribution': conv.pipe(
                conv.cleanup_line,
                conv.translate({
                    u'_': None,
                    }),
                conv.test_equals(u'zip'),
                ),
            u'dcat:keywords': conv.pipe(
                conv.function(lambda value: value.split(u',')),
                conv.uniform_sequence(
                    conv.input_to_slug,
                    constructor = lambda value: sorted(set(value)),
                    drop_none_items = True,
                    ),
                conv.empty_to_none,
                ),
            u'dcat:size': conv.pipe(
                conv.input_to_int,
                conv.test_greater_or_equal(0),
                conv.translate({
                    0: None,
                    }),
                ),
            u'dcat:theme': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:accrualPeriodicity': conv.pipe(
                conv.cleanup_line,
                conv.test_in(frequency_by_accrualPeriodicity),
                ),
            u'dct:contributor': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:creator': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:description': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:format': conv.pipe(
                conv.function(lambda value: value.split(u',')),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.cleanup_line,
                        conv.translate({
                            u'_': None,
                            }),
                        conv.test_in([
                            u'csv',
                            u'gtfs',
                            u'jpg',
                            u'json',
                            u'kml',
                            u'mapinfo',
                            u'pdf',
                            u'trident',
                            u'xls',
                            ]),
                        conv.function(lambda value: value.upper()),
                        ),
                    constructor = lambda value: sorted(set(value)),
                    drop_none_items = True,
                    ),
                conv.empty_to_none,
                ),
            u'dct:identifier': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:issued': conv.pipe(
                french_input_to_date,
                conv.date_to_iso8601_str,
                conv.not_none,
                ),
            u'dct:licence': conv.pipe(
                conv.input_to_slug,
                conv.test_in(license_id_by_licence),
                conv.not_none,
                ),
            u'dct:modified': conv.pipe(
                french_input_to_date,
                conv.date_to_iso8601_str,
                conv.not_none,
                ),
            u'dct:publisher': conv.pipe(
                conv.cleanup_line,
                conv.test_equals(u"Toulouse métropole"),
                conv.not_none,
                ),
            u'dct:spacial': conv.first_match(
                conv.pipe(
                    conv.test_equals(u"""\
Aucamville,Aussonne,Balma,Beaupuy,Beauzelle,Blagnac,Brax,Bruguieres,Castelginest,Colomiers,Cornebarrieu,Cugnaux,\
Fenouillet,Flourens, Fonbeauzard,Gagnac sur Garonne,Gratentour,L'Union,Launaguet,Lespinasse,Mondonville,Mondouzil,\
Montrabe,Pibrac,Pin Balma,Quint Fonsegrives,Saint Alban,Saint Jean,Saint Jory,Saint Orens de Gameville,Seilh,Toulouse,\
Tournefeuille,Villeneuve Tolosane"""),
                    conv.set_value(u'IntercommunalityOfFrance/243100518/CU DU GRAND TOULOUSE'),
                    ),
                conv.pipe(
                    conv.function(lambda value: value.split(u',')),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.cleanup_line,
                            str_to_territory,
                            conv.function(lambda territory: u'{}/{}/{}'.format(territory['kind'], territory['code'],
                                territory['main_postal_distribution'])),
                            ),
                        constructor = lambda value: sorted(set(value)),
                        drop_none_items = True,
                        ),
                    conv.empty_to_none,
                    conv.function(lambda value: u','.join(value)),
                    ),
                ),
            u'dct:title': conv.pipe(
                conv.cleanup_line,
                conv.not_none,
                ),
            u'Theme Inspire': conv.pipe(
                # To ignore.
                conv.cleanup_line,
                ),
            },
        ),
    )


# Functions


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
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
    global conf
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
                'territoria.site_url': conv.pipe(
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
        admin_name = u'michael-dot-combes-at-toulouse-metropole-dot-fr',
        supplier_abbreviation = u'tm',
        supplier_title = u'Toulouse-Métropole.data',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://data.toulouse-metropole.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    datasets_dataset_name = u'17709-catalogue-des-donnees-dataset-des-dataset'
    request = urllib2.Request(urlparse.urljoin(source_site_url,
        u'web/guest/les-donnees/-/opendata/card/{}/resource/document'.format(datasets_dataset_name)),
        headers = source_headers)
    response = urllib2.urlopen(request)
    zip_bytes = response.read()
    zip_archive = zipfile.ZipFile(cStringIO.StringIO(zip_bytes))
    filenames = zip_archive.namelist()
    assert len(filenames) == 1, filenames
    datasets_file = zip_archive.open(filenames[0])
    packages_csv_reader = csv.reader(datasets_file, delimiter = ';', quotechar = '"')
    labels = [
        cell.decode('cp1252').strip()
        for cell in packages_csv_reader.next()
        ]

    creators = set()
    for row in packages_csv_reader:
        row = [
            cell.decode('cp1252')
            for cell in row
            ]
        dataset = conv.check(entry_to_dataset)(dict(zip(labels, row)), state = conv.default_state)

        creators.add(dataset['dct:creator'])

        if dataset[u'dct:creator'] in organizations_title_to_ignore:
            continue

        if dataset[u'dcat:distribution'] == u'zip':
            resource = dict(
                created = dataset['dct:issued'],
                description = u'Archive contenant des fichiers aux formats: {}.'.format(
                    u', '.join(dataset[u'dct:format'])),
                format = 'ZIP',
                last_modified = dataset['dct:modified'],
                name = u'Fichier ZIP',
                url = dataset[u'dcat:dataset'].rstrip(u'/') + u'/resource/document',
                )
        else:
            resource = dict(
                created = dataset['dct:issued'],
                format = 'API',
                last_modified = dataset['dct:modified'],
                name = u'Service web',
                url = dataset[u'dcat:accessUrl'],
                )

        package = dict(
            author = dataset['dct:contributor'],
            frequency = frequency_by_accrualPeriodicity.get(dataset[u'dct:accrualPeriodicity']),
            license_id = license_id_by_licence[dataset[u'dct:licence']],
            notes = dataset[u'dct:description'],
            resources = [resource],
            tags = [
                dict(name = tag_name)
                for tag_name in (dataset[u'dcat:keywords'] or [])
                ],
            territorial_coverage = dataset[u'dct:spacial'],
            # territorial_coverage_granularity = 
            title = dataset[u'dct:title'],
            url = dataset[u'dcat:dataset'],
            )

        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = dataset[u'dcat:theme'],
                    )),
                harvester.upsert_group(dict(
                    title = u'Territoires et Transports',
                    )),
                ]

            organization = harvester.upsert_organization(dict(
                title = organization_title_translations.get(dataset['dct:creator'], dataset['dct:creator']),
                ))

            harvester.add_package(package, organization, dataset[u'dct:identifier'], package[u'url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Organizations: {}'.format(u', '.join(sorted(creators))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
