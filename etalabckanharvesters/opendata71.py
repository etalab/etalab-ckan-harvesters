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


"""Harvest open data from "Open data 71".

http://www.opendata71.fr/ & http://opendata71interactive.cloudapp.net/
"""


import argparse
import ConfigParser
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states, strings

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, jsonconv, states)
frequency_by_updatefrequency = {
    u'1 year': u"annuelle",
    u'never': u"aucune",
    u'weekly': u"hebdomadaire",
    }
log = logging.getLogger(app_name)
N_ = lambda message: message
territorial_coverage_by_geographiccoverage = {
    u'Archives départementales de Saône-et-Loire': u'DepartmentOfFrance/71',
    u'Bourgogne': u'RegionOfFrance/26',
    u'Département de Saône-et-Loire': u'DepartmentOfFrance/71',
    u'Epinac (Saône-et-Loire)': u'CommuneOfFrance/71190',
    u'France': u'Country/FR',
    u'France et étranger': u'InternationalOrganization/WW',
    u'France et pays étrangers': u'InternationalOrganization/WW',
    u'Mâcon': u'CommuneOfFrance/71270',
    u'Musées départementaux 71 : Solutré et Romaneche-Thorins': u'DepartmentOfFrance/71',
    u'Saône et Loire': u'DepartmentOfFrance/71',
    u'Saône-et Loire': u'DepartmentOfFrance/71',
    u'Saône-et-Loire': u'DepartmentOfFrance/71',
    u'Saône-et-Loire / Bourgogne': u'RegionOfFrance/26',
    u'Saône-et-Loire (26 villes)': u'DepartmentOfFrance/71',
    u'Saône-et-Loire (ensemble des communes du département)': u'DepartmentOfFrance/71',
    }
uuid_re = re.compile(ur'[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$')
year_period_re = re.compile(ur'(?P<year_from>\d{4})\s*[-à]\s*(?P<year_to>\d{4})$')
year_re = re.compile(ur'(?P<year>\d{4})$')


json_to_uuid = conv.pipe(
    conv.test_isinstance(basestring),
    conv.test(uuid_re.match, error = N_(u'Invalid ID')),
    )


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
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
        supplier_abbreviation = u'od71',
        supplier_title = u'Open Data 71',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_html_url = u'http://opendata71interactive.cloudapp.net/'
    source_site_url = u'http://opendata71.cloudapp.net/'

    harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    collection_name = 'data'
    request = urllib2.Request(urlparse.urljoin(source_site_url, u'v1/{}/TableMetadata?$format=json'.format(
        collection_name)), headers = source_headers)
    response = urllib2.urlopen(request)
    table_metadata = conv.check(conv.pipe(
        conv.make_input_to_json(),
        conv.test_isinstance(dict),
        conv.struct(
            dict(
                d = conv.pipe(
                    conv.test_isinstance(list),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.test_isinstance(dict),
                            conv.not_none,
                            ),
                        ),
                    conv.not_none,
                    ),
                ),
            ),
        ))(response.read(), state = conv.default_state)
    for entry in table_metadata['d']:
        entry = conv.check(conv.struct(
            dict(
                additionalinfo = conv.test_isinstance(basestring),
                category = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                collectioninstruments = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    ),
                collectionmode = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.test_none(),
                    ),
                datadictionary_variables = conv.test_isinstance(basestring),
                description = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                entityid = conv.pipe(
                    json_to_uuid,
                    conv.not_none,
                    ),
                entitykind = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                entityset = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                expireddate = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.not_none,
                    ),
                geographiccoverage = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.test_in(territorial_coverage_by_geographiccoverage),
                    ),
                isempty = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.test_in([
                        u'false',
                        u'true',
                        ]),
                    conv.test_equals(u'false'),
                    conv.not_none,
                    ),
                keywords = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.function(lambda s: s.split(u',')),
                    conv.uniform_sequence(
                        conv.cleanup_line,
                        drop_none_items = True,
                        ),
                    conv.empty_to_none,
                    ),
                lastupdatedate = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.not_none,
                    ),
                links = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.make_input_to_url(full = True),
                    ),
                metadataurl = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    ),
                name = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                PartitionKey = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                periodcovered = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
#                    conv.first_match(
#                        conv.test_in([
#                            u'19e siècle',
#                            u'20e siècle',
#                            u'20e siècle - 21e siècle',
#                            u'Du 11e au 21e siècle',
#                            u'Du 7 au 9 juin 2013',
#                            u'Préhistoire - 1995',
#                        ]),
#                        conv.test(year_period_re.match),
#                        conv.test(year_re.match),
#                        ),
                    ),
                releaseddate = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.not_none,
                    ),
                RowKey = conv.pipe(
                    json_to_uuid,
                    conv.not_none,
                    ),
                source = conv.pipe(
                    conv.test_isinstance(basestring),
#                    conv.test_in([
#                        u'Agence de développement touristique',
#                        u'Agence du développement touristique',
#                        u'Archives départementales de Saône-et-Loire',
#                        u'BaladesVertes',
#                        u'CG71SdisAppels',
#                        u"Direction de l'aménagement durable des territoires et de l'environnement (DADTE)",
#                        u"Direction de l'aménagement durable des territoires et de l'environnement  (DADTE)",
#                        u"Direction de l'insertion et du logement social (DILS)",
#                        u'Direction de la Lecture Publique',
#                        u'Direction de la lecture publique',
#                        u'Direction de la Lecture Publique (DLP)',
#                        u"Direction des finances et de l'évaluation des gestions",
#                        u'Direction des routes et des infrastructures (DRI)',
#                        u"Direction des Transports et de l'intermodalité (DTI)",
#                        u"Direction du développement rural et de l'agriculture (DDRA)",
#                        u"Service de l'information géographique",
#                        u'Tableau de bord de la Direction générale',
#                        ]),
                    conv.empty_to_none,
                    ),
                technicalinfo = conv.test_isinstance(basestring),
                Timestamp = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.iso8601_input_to_datetime,
                    conv.not_none,
                    ),
                updatefrequency = conv.pipe(
                    conv.test_isinstance(basestring),
                    conv.empty_to_none,
                    conv.test_in(frequency_by_updatefrequency),
                    ),
                ),
            ))(entry, state = conv.default_state)

        groups = [
            harvester.upsert_group(dict(
                title = entry[u'category'],
                )),
            harvester.upsert_group(dict(
                title = u'Territoires et Transports',
                )),
            ]

        html_url = urlparse.urljoin(source_site_html_url, u'DataBrowser/{}/{}'.format(collection_name,
            entry[u'entityset']))

        temporal_coverage_from = None
        temporal_coverage_to = None
        if entry[u'periodcovered'] is not None:
            match = year_period_re.match(entry[u'periodcovered'])
            if match is not None:
                temporal_coverage_from = match.group('year_from')
                temporal_coverage_to = match.group('year_to')
            else:
                match = year_re.match(entry[u'periodcovered'])
                if match is not None:
                    temporal_coverage_from = match.group('year')
                    temporal_coverage_to = match.group('year')

        package = dict(
            author = entry[u'source'],
            frequency = frequency_by_updatefrequency.get(entry[u'updatefrequency']),
            license_id = u'other-at',
            notes = entry[u'description'],
            resources = [
                dict(
                    description = u"""\
La page HTML permet de télécharger les données sous différents formats dont le CSV.
Une API permet de manipuler les données en JSON et XML.
""",
                    format = u'HTML',
                    name = entry[u'PartitionKey'],
                    url = html_url,
                    ),
                ],
            tags = [
                dict(name = strings.slugify(tag_name))
                for tag_name in (entry[u'keywords'] or [])
                ],
            temporal_coverage_from = temporal_coverage_from,
            temporal_coverage_to = temporal_coverage_to,
            territorial_coverage = territorial_coverage_by_geographiccoverage.get(entry[u'geographiccoverage']),
            title = entry[u'name'],
            # url = ...,  # url is the same as for the resource => ignore it.
            )
        organization = harvester.upsert_organization(dict(
            title = u'Conseil général de Saône-et-Loire (CG71)',
            ))

        harvester.add_package(package, organization, entry[u'entityset'], html_url, groups = groups)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
