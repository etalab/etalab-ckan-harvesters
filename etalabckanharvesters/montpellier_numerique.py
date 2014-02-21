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


"""Harvest open data from "Montpelllier Territoire Numérique".

http://opendata.montpelliernumerique.fr/Les-donnees
"""


import argparse
import collections
import ConfigParser
import csv
import itertools
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, states, strings
from lxml import etree

from . import helpers

accrual_periodicity_translations = {
    u"Annuelle": u"annuelle",
    u"Hebdomadaire": u"hebdomadaire",
    u"Journalière": u"quotidienne",
    u"ponctuelle": u"ponctuelle",
    }
app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, states)
log = logging.getLogger(app_name)
name_re = re.compile(u'(\{(?P<url>.+)\})?(?P<name>.+)$')


xml_python_to_french_text = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        {
            u'@xml:lang': conv.pipe(
                conv.test_isinstance(basestring),
                conv.test_equals(u'fr'),
                conv.not_none,
                ),
            u'^text': conv.pipe(
                conv.test_isinstance(basestring),
                conv.not_none,
                ),
            },
        ),
    conv.function(lambda value: value[u'^text']),
    )

validate_xml_python = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        {
            u'@rdf:about': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                conv.not_none,
                ),
            u'dcat:distribution': conv.pipe(
                conv.make_item_to_singleton(),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.test_isinstance(dict),
                        conv.struct(
                            {
                                u'dcat:Distribution': conv.pipe(
                                    conv.test_isinstance(dict),
                                    conv.struct(
                                        {
                                            u'dcat:accessURL': conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.make_input_to_url(full = True),
                                                conv.not_none,
                                                ),
                                            u'dct:format': conv.pipe(
                                                conv.test_isinstance(dict),
                                                conv.struct(
                                                    {
                                                        u'dct:IMT': conv.pipe(
                                                            conv.test_isinstance(dict),
                                                            conv.struct(
                                                                {
                                                                    u'rdfs:label': conv.pipe(
                                                                        conv.test_isinstance(basestring),
                                                                        conv.test_in([
                                                                            u'CSV',
                                                                            u'DATABASE',
                                                                            u'DOC',
                                                                            u'ECW',
                                                                            u'JSON',
                                                                            u'KML',
                                                                            u'ODS',
                                                                            u'ODT',
                                                                            u'PDF',
                                                                            u'PPT',
                                                                            u'RDF',
                                                                            u'SHP',
                                                                            u'TXT',
                                                                            u'XLS',
                                                                            u'XML',
                                                                            ]),
                                                                        conv.not_none,
                                                                        ),
                                                                    },
                                                                ),
                                                            conv.not_none,
                                                            ),
                                                        },
                                                    ),
                                                conv.not_none,
                                                ),
                                            u'dct:title': conv.pipe(
                                                xml_python_to_french_text,
                                                conv.not_none,
                                                ),
                                            u'rdf:type': conv.pipe(
                                                conv.test_isinstance(dict),
                                                conv.struct(
                                                    {
                                                        '@rdf:resource': conv.pipe(
                                                            conv.test_isinstance(basestring),
                                                            conv.make_input_to_url(full = True),
                                                            conv.test_in([
                                                                u'http://www.w3.org/ns/dcat#Download',
                                                                ]),
                                                            conv.not_none,
                                                            ),
                                                        },
                                                    ),
                                                conv.not_none,
                                                ),
                                            },
                                        ),
                                    conv.not_none,
                                    ),
                                },
                            ),
                        conv.not_none,
                        ),
                    ),
                conv.empty_to_none,
                conv.not_none,
                ),
            u'dcat:Download': conv.test_none(),
            u'dcat:keyword': conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        xml_python_to_french_text,
                        conv.not_none,
                        ),
                    ),
                ),
            u'dcat:theme': conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        xml_python_to_french_text,
                        conv.not_none,
                        ),
                    ),
                ),
            u'dct:accrualPeriodicity': conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    {
                        u'dct:Frequency': conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                {
                                    u'rdfs:label': conv.pipe(
                                        xml_python_to_french_text,
                                        conv.test_in(accrual_periodicity_translations),
                                        conv.not_none,
                                        ),
                                    },
                                ),
                            conv.not_none,
                            ),
                        },
                    ),
                ),
            u'dct:description': conv.pipe(
                xml_python_to_french_text,
                conv.not_none,
                ),
            u'dct:identifier': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            u'dct:issued': conv.pipe(
                conv.test_isinstance(list),
                conv.uniform_sequence(
                    conv.pipe(
                        conv.iso8601_input_to_date,
                        conv.date_to_iso8601_str,
                        conv.not_none,
                        ),
                    constructor = sorted,
                    ),
                conv.empty_to_none,
                conv.not_none,
                ),
            u'dct:language': conv.pipe(
                xml_python_to_french_text,
                conv.test_in([u'anglais', u'français']),
                conv.not_none,
                ),
            u'dct:licence': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                conv.test_in([
                    u'http://www.etalab.gouv.fr/pages/licence-ouverte-open-licence-5899923.html',
                    ]),
                conv.not_none,
                ),
            u'dct:publisher': conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    {
                        u'foaf:Organization': conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                {
                                    u'dct:title': conv.pipe(
                                        conv.test_isinstance(basestring),
                                        conv.test_in([
                                            u"AIR LR",
                                            u"ASF",
                                            u"CCI Territoire de Montpellier",
                                            u"INSEE",
                                            u"OpenStreetMap",
                                            u"TELA BOTANICA",
                                            u"Ville de Montpellier",
                                            ]),
                                        conv.not_none,
                                        ),
                                    u'foaf:homepage': conv.pipe(
                                        conv.test_isinstance(dict),
                                        conv.struct(
                                            {
                                                u'@rdf:resource': conv.pipe(
                                                    conv.test_isinstance(basestring),
                                                    conv.make_input_to_url(add_prefix = u'http://',
                                                        full = True),
                                                    ),
                                                },
                                            ),
                                        conv.not_none,
                                        ),
                                    },
                                ),
                            conv.not_none,
                            ),
                        },
                    ),
                conv.not_none,
                ),
            u'dct:references': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                ),
            u'dct:spatial': conv.pipe(
                xml_python_to_french_text,
                conv.test_in([
                    u"Hérault",
                    u"Montpellier",
                    u"Région Languedoc-Roussillon",
                    u"Ville de Montpellier",
                    ]),
                conv.not_none,
                ),
            u'dct:title': conv.pipe(
                xml_python_to_french_text,
                conv.not_none,
                ),
            u'rdfs:seeAlso': conv.pipe(
                conv.test_isinstance(basestring),
                conv.make_input_to_url(full = True),
                conv.not_none,
                ),
            },
        ),
    )


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
        old_supplier_title = u'Ville de Montpellier',
        supplier_abbreviation = u'mtn',
        supplier_title = u'Montpellier Territoire Numérique',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://opendata.montpelliernumerique.fr/'

    harvester.retrieve_target()

    # Retrieve short infos of packages in source.
    log.info(u'Retrieving list of source datasets')
    request = urllib2.Request(urlparse.urljoin(source_site_url, '?page=opendata_extract'), headers = source_headers)
    response = urllib2.urlopen(request)
    datasets_csv_reader = csv.reader(response, delimiter = ',', quotechar = '"')
    while True:
        labels = datasets_csv_reader.next()
        if not labels or len(labels) == 1 and not labels[0].strip():
            continue
        break
    packages_source_id = set()
    for row in datasets_csv_reader:
        if not row or len(row) == 1 and not row[0].strip():
            continue
        record = dict(
            (label.decode('utf-8'), cell.decode('utf-8'))
            for label, cell in zip(labels, row)
            )
        record = conv.check(conv.struct(
            dict(
                ID = conv.pipe(
                    conv.input_to_int,
                    conv.not_none,
                    ),
                Statut = conv.pipe(
                    conv.test_in([u'prop', u'pub', u'refuse']),
                    ),
                ),
            default = conv.noop,
            ))(record, state = conv.default_state)
        if record[u'Statut'] == u'pub':
            packages_source_id.add(record['ID'])

    # Retrieve packages from source.
    for package_source_id in sorted(packages_source_id):
        request = urllib2.Request(urlparse.urljoin(source_site_url, 'meta/export_rdf/{}'.format(package_source_id)),
            headers = source_headers)
        response = urllib2.urlopen(request)
        rdf_doc = etree.parse(response)
        source_package = convert_xml_element_to_python(rdf_doc.getroot())['dcat:Dataset']
        source_package = conv.check(conv.pipe(
            validate_xml_python,
            conv.not_none,
            ))(source_package, state = conv.default_state)

        organization_title = source_package[u'dct:publisher'][u'foaf:Organization'][u'dct:title']
        if organization_title not in (
                u"AIR LR",
                u"ASF",
                u"CCI Territoire de Montpellier",
                u"Ville de Montpellier",
                ):
            if organization_title not in (
                    u"INSEE",
                    u"OpenStreetMap",
                    u"TELA BOTANICA",
                    ):
                log.warning(u'Ignoring package "{}" from "{}"'.format(source_package['dct:title'], organization_title))
            continue
        organization = harvester.upsert_organization(dict(
            title = organization_title,
            url = source_package[u'dct:publisher'][u'foaf:Organization'][u'foaf:homepage'][u'@rdf:resource'],
            ))

        resources = []
        for distribution in source_package[u'dcat:distribution']:
            sub_distribution = distribution[u'dcat:Distribution']
            resources.append(dict(
                created = source_package[u'dct:issued'][0],
                name = sub_distribution[u'dct:title'],
                format = sub_distribution[u'dct:format'][u'dct:IMT'][u'rdfs:label'].lower(),
                last_modified = source_package[u'dct:issued'][-1] if len(source_package[u'dct:issued']) > 1 else None,
                url = sub_distribution[u'dcat:accessURL'],
                ))

        source_url = source_package[u'@rdf:about'].replace(u'../', u'')
        themes_list = source_package[u'dcat:theme']

        package = dict(
            license_id = u'fr-lo',
            notes = source_package[u'dct:description'],
            resources = resources,
            tags = [
                dict(name = strings.slugify(tag_title))
                for tag_title in itertools.chain(
                    source_package[u'dcat:keyword'],
                    (
                        theme
                        for themes in themes_list
                        for theme in themes.split(u', ')
                        ),
                    )
                ],
            territorial_coverage = {
                u"Hérault": u'DepartmentOfFrance/34/34 HERAULT',
                u"Montpellier": u'IntercommunalityOfFrance/243400017/CA DE MONTPELLIER',
                u"Région Languedoc-Roussillon": u'RegionOfFrance/91/LANGUEDOC ROUSSILLON',
                u"Ville de Montpellier": u'CommuneOfFrance/34172/34000 MONTPELLIER',
                }[source_package[u'dct:spatial']],
            title = source_package['dct:title'],
            url = source_url,
            )

        accrual_periodicity = ((source_package[u'dct:accrualPeriodicity'] or {}).get(u'dct:Frequency') or {}).get(
            u'rdfs:label')
        if accrual_periodicity is not None:
            package['frequency'] = accrual_periodicity_translations[accrual_periodicity]

        helpers.set_extra(package, u'Identifiant', source_package[u'dct:identifier'])
        helpers.set_extra(package, u'Langue', source_package[u'dct:language'])
        helpers.set_extra(package, u'Référence', source_package[u'dct:references'])

        if themes_list:
            groups = [
                harvester.upsert_group(dict(
                    title = themes_list[0],
                    )),
                ]
            if len(themes_list) > 1:
                helpers.set_extra(package, u'Thème', themes_list[1])
        else:
            groups = []
        groups.append(harvester.upsert_group(dict(
            title = u'Territoires et Transports',
            )))

        log.info(u'Harvested package: {}'.format(package['title']))
        harvester.add_package(package, organization, source_package['dct:title'], source_url, groups = groups)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
