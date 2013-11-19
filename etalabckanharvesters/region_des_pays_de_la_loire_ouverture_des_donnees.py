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


"""Harvest open data from "Région des Pays de la Loire ouverture des données publiques".

http://data.paysdelaloire.fr/
"""


import argparse
import collections
import ConfigParser
import datetime
import itertools
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, jsonconv, states
from lxml import etree

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, jsonconv, states)
frequency_by_temporal = {
    u'actualisation-prevue-en-2015': u'ponctuelle',
    u'annuel': u'annuelle',
    u'annuelle': u'annuelle',
    u'annuelle-hors-ter-mensuelle-ter': u'annuelle',  # ???
    u'aucune': u'aucune',
    u'bi-annuelle': u'semestrielle',
    u'hebdomadaire': u'hebdomadaire',
    u'journaliere': u'quotidienne',
    u'lorsque-necessaire': u'ponctuelle',
    u'lorsque-nessaire': u'ponctuelle',
    u'mensuelle': u'mensuelle',
    u'par-commision-permanente': u'ponctuelle',
    u'par-commission-parmanente': u'ponctuelle',
    u'par-commission-permanante': u'ponctuelle',
    u'par-commission-permanente': u'ponctuelle',
    u'publication-unique': u'aucune',
    u'quotidienne': u'quotidienne',
    u'selon-changement-dans-l-institution': u'ponctuelle',
    u'selon-vote': u'ponctuelle',
    u'semestrielle': u'semestrielle',
    u'semestrielle-et-ou-a-l-occasion-de-modification-majeure': u'semestrielle',
    u'temps-reel': u'temps réel',
    u'temps-reel-5-minutes': u'temps réel',
    u'tous-les-5-ans': u'quinquennale',
    u'trimestrielle': u'trimestrielle',
    u'variable': u'ponctuelle',
    u'variable-ponctuelle': u'ponctuelle',
    }
granularity_translations = {
    u'commune': u'commune',
    u'precision-geometrique-5-metres': u'poi',
    u'region': u'region',
    }
license_id_by_title = {
    u'Open Database License (ODbL)': u'odc-odbl',
    }
log = logging.getLogger(app_name)
md5_re = re.compile(ur'[\da-f]{32}')
name_re = re.compile(u'(\{(?P<url>.+)\})?(?P<name>.+)$')
organization_title_translations = {
#    u'Air Pays de la Loire',
#    u'Banque de France',
#    u'Département de Loire-Atlantique',
#    u'Destinéo',
#    u'Loire-Atlantique Tourisme',
#    u'Musique et Danse en Loire-Atlantique',
#    u'Nantes Métropole',
#    u'OpenStreetMap',
#    u'Région des Pays de la Loire',
#    u'Semitan',
    u'SNCF': u'Société nationale des chemins de fer français',
#    u'Ville de Nantes',
    }
organizations_title_to_ignore = [
    u'INSEE',
    u"Ministère de l'Economie et des Finances",
    u"Ministère de l'Intérieur",
    ]
territorial_coverage_by_spatial = {
    u'loire-atlantique': u'DepartmentOfFrance/44/LOIRE ATLANTIQUE',
    u'nantes': u'CommuneOfFrance/44109/44000 NANTES',
    u'nantes-metropole': u'IntercommunalityOfFrance/244400404/CU NANTES METROPOLE',
    u'pays-de-la-loire': u'RegionOfFrance/52/PAYS DE LA LOIRE',
    u'region-des-pays-de-la-loire': u'RegionOfFrance/52/PAYS DE LA LOIRE',
    u'ville-de-nantes': u'CommuneOfFrance/44109/44000 NANTES',
    }

validate_xml_python = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        {
            u'@rdf:about': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                conv.function(lambda url: url.replace(u'/opendata/', u'/', 1)),
                conv.not_none,
                ),
            u'dcat:accessURL': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                ),
            u'dcat:dataQuality': conv.test_none(),
            u'dcat:dataset': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                conv.function(lambda url: url.replace(u'/opendata/', u'/', 1)),
                conv.not_none,
                ),
            u'dcat:distribution': conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    {
                        u'dcat:Distribution': conv.pipe(
                            conv.make_item_to_singleton(),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.test_isinstance(dict),
                                    conv.struct(
                                        {
                                            u'dcat:accessURL': conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.make_input_to_url(full = True),
                                                conv.function(lambda url: url.replace(u'//api/', u'/api/')),
                                                conv.not_none,
                                                ),
                                            u'dcat:Download': conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.make_input_to_url(),
                                                ),
                                            u'dcat:WebService': conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.make_input_to_url(),
                                                ),
                                            u'dct:format': conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.test_in([
                                                    u'API',
                                                    u'CSV',
                                                    u'GTFS',
                                                    u'JSON',
                                                    u'JPG',
                                                    u'KML',
                                                    u'KMZ',
                                                    u'MIF',
                                                    u'PDF',
                                                    u'SHP (CC47)',
                                                    u'SHP (L93)',
                                                    u'XLS',
                                                    u'XML',
                                                    ]),
                                                conv.not_none,
                                                ),
                                            },
                                        ),
                                    conv.not_none,
                                    ),
                                ),
                            conv.empty_to_none,
                            ),
                        },
                    ),
                conv.function(lambda value: value[u'dcat:Distribution']),
                ),
            u'dcat:granularity': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_slug,
                conv.test_in(granularity_translations),
                ),
            u'dcat:keywords': conv.pipe(
                conv.test_isinstance(basestring),
                conv.function(lambda value: value.split(u',')),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.input_to_slug,
                        conv.not_none,
                        ),
                    ),
                ),
            u'dcat:theme': conv.pipe(
                conv.test_isinstance(basestring),
                conv.function(lambda value: value.split(u'/')),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.cleanup_line,
                        conv.not_none,
                        ),
                    ),
                ),
            u'dcat:themeTaxonomy': conv.test_none(),
            u'dct:created': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_float,
                conv.function(datetime.datetime.fromtimestamp),
                conv.datetime_to_iso8601_str,
                conv.not_none,
                ),
            u'dct:creator': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:description': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:identifier': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:issued': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_float,
                conv.function(datetime.date.fromtimestamp),
                conv.date_to_iso8601_str,
                conv.not_none,
                ),
            u'dct:language': conv.pipe(
                conv.test_isinstance(basestring),
                conv.test_in([u'fr', u'français']),
                conv.not_none,
                ),
            u'dct:licence': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.test_in(license_id_by_title),
                conv.not_none,
                ),
            u'dct:modified': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_float,
                conv.function(datetime.date.fromtimestamp),
                conv.date_to_iso8601_str,
                conv.not_none,
                ),
            u'dct:publisher': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:references': conv.pipe(
                # HTML fragment containing <a href="..">...</a>
                conv.test_isinstance(basestring),
                conv.cleanup_text,
                ),
            u'dct:spatial': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_slug,
                conv.function(lambda value: None if 'wgs84' in value else value),
                conv.test_in(territorial_coverage_by_spatial),
                ),
            u'dct:temporal': conv.pipe(
                conv.test_isinstance(basestring),
                conv.input_to_slug,
                conv.test_in(frequency_by_temporal),
                conv.not_none,
                ),
            u'dct:title': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'foaf:name': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'themeInspire': conv.pipe(
                # To ignore.
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                ),
            u'lastModificationDescription': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                ),
            },
        ),
    )


def convert_xml_element_to_python(value):
    if value is None:
        return value
    element = collections.OrderedDict(
        (u'@' + convert_xml_name_to_python(value.nsmap, attribute_name), attribute_value)
        for attribute_name, attribute_value in value.attrib.iteritems()
        )
    children = list(value)
    if children:
        for child in children:
            if child.tag in (etree.Comment, etree.PI):
                continue
            child_tag = convert_xml_name_to_python(child.nsmap, child.tag)
            if child_tag in element:
                same_tag_children = element[child_tag]  # either a single child or a list of children
                if isinstance(same_tag_children, list):
                    same_tag_children.append(convert_xml_element_to_python(child))
                else:
                    element[child_tag] = [
                        same_tag_children,
                        convert_xml_element_to_python(child),
                        ]
            else:
                element[child_tag] = convert_xml_element_to_python(child)
    elif value.text is not None and value.text.strip() and value.tail is not None and value.tail.strip():
        assert 'text' not in element  # TODO
        element['^text'] = value.text
        assert 'tail' not in element  # TODO
        element['^tail'] = value.tail
    elif element:
        if value.text is not None and value.text.strip():
            assert 'text' not in element  # TODO
            element['^text'] = value.text
        if value.tail is not None and value.tail.strip():
            assert 'tail' not in element  # TODO
            element['^tail'] = value.tail
    elif value.text is not None and value.text.strip():
        element = value.text
    elif value.tail is not None and value.tail.strip():
        element = value.tail
    else:
        element = None
    return element


def convert_xml_name_to_python(nsmap, value):
    if value is None:
        return value
    match = name_re.match(value)
    url = match.group('url')
    for namespace_name, namespace_url in itertools.chain(
            [('xml', 'http://www.w3.org/XML/1998/namespace')],
            nsmap.iteritems(),
            ):
        if url == namespace_url:
            return u'{}:{}'.format(namespace_name, match.group('name'))
    return value


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-d', '--dry-run', action = 'store_true',
        help = "simulate harvesting, don't update CKAN repository")
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
        supplier_abbreviation = u'laod',
        supplier_title = u'Région des Pays de la Loire ouverture des données publiques',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://data.paysdelaloire.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    request_url = urlparse.urljoin(source_site_url, u'api/datastore_searchdatasets/1.0/KKQOL1H5VC0P50J/?output=json')
    request = urllib2.Request(request_url, headers = source_headers)
    response = urllib2.urlopen(request)
    datasets_id = conv.check(conv.pipe(
        make_input_to_response_data(request_url),
        conv.struct(
            dict(
                dataset = conv.pipe(
                    conv.test_isinstance(list),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                dict(
                                    id = conv.pipe(
                                        conv.test_isinstance(basestring),
                                        conv.input_to_int,
                                        conv.not_none,
                                        ),
                                    ),
                                ),
                            conv.not_none,
                            ),
                        ),
                    conv.empty_to_none,
                    ),
                ),
            ),
        conv.function(lambda value: [item['id'] for item in value['dataset']]),
        conv.not_none,
        ))(response.read(), state = conv.default_state)

    for dataset_id in datasets_id:
        request_url = urlparse.urljoin(source_site_url,
            u'api/datastore_getdatasets/1.0/KKQOL1H5VC0P50J/?param[ids]={}&output=rdf'.format(dataset_id))
        request = urllib2.Request(request_url, headers = source_headers)
        response = urllib2.urlopen(request)
        rdf_doc = etree.parse(response)
        dataset = convert_xml_element_to_python(rdf_doc.getroot())['dcat:Dataset']
        entry = conv.check(conv.pipe(
            validate_xml_python,
            conv.not_none,
            ))(dataset, state = conv.default_state)

        if entry[u'dct:creator'] in organizations_title_to_ignore:
            continue

        package = dict(
            # author = entry['dct:creator'],
            frequency = frequency_by_temporal[entry[u'dct:temporal']],
            license_id = license_id_by_title[entry[u'dct:licence']],
            notes = entry[u'dct:description'],
            resources = [
                dict(
                    created = entry['dct:issued'],
                    format = distribution['dct:format'],
                    last_modified = entry['dct:modified'],
                    name = u'{}.{}'.format(entry['dct:identifier'], distribution['dct:format']),
                    url = distribution[u'dcat:accessURL'],
                    )
                for distribution in (entry[u'dcat:distribution'] or [])
                ],
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(entry[u'dcat:keywords'] or []))
                ],
            territorial_coverage = territorial_coverage_by_spatial.get(entry[u'dct:spatial']),
            territorial_coverage_granularity = granularity_translations.get(entry[u'dcat:granularity']),
            title = entry[u'dct:title'],
            url = entry[u'dcat:dataset'],
            )

        log.info(u'Harvested package: {}'.format(package['title']))
        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = group_title,
                    ))
                for group_title in (entry[u'dcat:theme'] or [])
                ]

            organization = harvester.upsert_organization(dict(
                title = organization_title_translations.get(entry['dct:creator'], entry['dct:creator']),
                ))

            harvester.add_package(package, organization, entry[u'dct:identifier'], entry[u'dcat:dataset'],
                groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


def make_input_to_response_data(request_url):
    return conv.pipe(
        conv.make_input_to_json(),
        conv.test_isinstance(dict),
        conv.struct(
            dict(
                opendata = conv.pipe(
                    conv.test_isinstance(dict),
                    conv.struct(
                        dict(
                            answer = conv.pipe(
                                conv.test_isinstance(dict),
                                conv.struct(
                                    dict(
                                        data = conv.pipe(
                                            conv.test_isinstance(dict),
                                            conv.not_none,
                                            ),
                                        status = conv.pipe(
                                            conv.test_isinstance(dict),
                                            conv.struct(
                                                {
                                                    u'@attributes': conv.pipe(
                                                        conv.test_isinstance(dict),
                                                        conv.struct(
                                                            dict(
                                                                code = conv.pipe(
                                                                    conv.test_equals('0'),
                                                                    conv.not_none,
                                                                    ),
                                                                message = conv.pipe(
                                                                    conv.test_equals('OK'),
                                                                    conv.not_none,
                                                                    ),
                                                                ),
                                                            ),
                                                        ),
                                                    },
                                                ),
                                            ),
                                        ),
                                    ),
                                conv.not_none,
                                ),
                            request = conv.pipe(
                                conv.test_isinstance(basestring),
                                conv.test_equals(request_url),
                                conv.not_none,
                                ),
                            ),
                        ),
                    conv.not_none,
                    ),
                ),
            ),
        conv.function(lambda value: value['opendata']['answer']['data']),
        )


if __name__ == '__main__':
    sys.exit(main())
