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


"""Harvest open data from "Ministère de l'agriculture".

https://fichiers-publics.agriculture.gouv.fr/
"""


import argparse
import collections
import ConfigParser
import itertools
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states
from lxml import etree
import lxml.html

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, states)
granularity_translations = {
    u'France': u'pays',
    }
log = logging.getLogger(app_name)
name_re = re.compile(u'(\{(?P<url>.+)\})?(?P<name>.+)$')
organization_title_translations = {
    u'Ministère de l’Agriculture, de l’Agroalimentaire et de la Forêt':
        u'Ministère de l’Agriculture, de l’Agroalimentaire et de la Forêt',
    }
territorial_coverage_translations = {
    u'Country/FR': u'Country/FR/FRANCE',
    }

validate_xml_python = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        {
            '@xsi:noNamespaceSchemaLocation': conv.pipe(
                conv.test_equals('ETALAB_schema_des_meta_donnees.xsd'),
                conv.not_none,
                ),
            'digest': conv.pipe(
                conv.test_isinstance(basestring),
                conv.cleanup_line,
                conv.not_none,
                ),
            'metadata': conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    dict(
                        author = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        author_email = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.input_to_email,
                            ),
                        extras = conv.pipe(
                            conv.make_item_to_singleton(),
                            conv.test_isinstance(list),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.test_isinstance(dict),
                                    conv.struct(
                                        dict(
                                            key = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.cleanup_line,
                                                conv.not_none,
                                                ),
                                            value = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.cleanup_line,
                                                conv.not_none,
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        frequency = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.test_in([
                                u"annuelle",
                                u"aucune",
                                u"bimensuelle",
                                u"bimestrielle",
                                u"hebdomadaire",
                                u"mensuelle",
                                u"ponctuelle",
                                u"quinquennale",
                                u"quotidienne",
                                u"semestrielle",
                                u"temps réel",
                                u"triennale",
                                u"trimestrielle",
                                ]),
                            conv.not_none,
                            ),
                        groups = conv.pipe(
                            conv.make_item_to_singleton(),
                            conv.test_isinstance(list),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.test_isinstance(basestring),
                                    conv.cleanup_line,
                                    conv.not_none,
                                    ),
                                ),
                            ),
                        id = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        license_id = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.test_in([
                                u'fr-lo',
                                ]),
                            conv.not_none,
                            ),
                        maintainer = conv.test_none(),
                        maintainer_email = conv.test_none(),
                        notes = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_text,
                            ),
                        organization = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_in(organization_title_translations),
                            conv.not_none,
                            ),
                        private = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.input_to_bool,
                            ),
                        resources = conv.pipe(
                            conv.make_item_to_singleton(),
                            conv.test_isinstance(list),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.test_isinstance(dict),
                                    conv.struct(
                                        dict(
                                            description = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.cleanup_text,
                                                ),
                                            format = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.cleanup_line,
                                                conv.test_in([
                                                    u"CLE",
                                                    u"CSV",
                                                    ]),
                                                conv.not_none,
                                                ),
                                            name = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.cleanup_line,
                                                conv.not_none,
                                                ),
                                            url = conv.pipe(
                                                conv.test_isinstance(basestring),
                                                conv.make_input_to_url(full = True),
                                                conv.not_none,
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            conv.not_none,
                            ),
                        state = conv.test_none(),
                        supplier = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.test_equals(u'Ministère de l’Agriculture, de l’Agroalimentaire et de la Forêt'),
                            conv.not_none,
                            ),
                        tags = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.function(lambda value: value.split(u',')),
                            conv.uniform_sequence(
                                conv.pipe(
                                    conv.input_to_slug,
                                    conv.not_none,
                                    ),
                                ),
                            ),
                        temporal_coverage_from = conv.test_none(),
                        temporal_coverage_to = conv.test_none(),
                        territorial_coverage = conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                dict(
                                    territorial_coverage_code = conv.pipe(
                                        conv.test_isinstance(basestring),
                                        conv.cleanup_line,
                                        conv.test_in(territorial_coverage_translations),
                                        conv.not_none,
                                        ),
                                    territorial_coverage_granularity = conv.pipe(
                                        conv.test_isinstance(basestring),
                                        conv.cleanup_line,
                                        conv.test_in(granularity_translations),
                                        conv.not_none,
                                        ),
                                    ),
                                ),
                            conv.not_none,
                            ),
                        title = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        ),
                    ),
                conv.not_none,
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
        supplier_abbreviation = u'agr',
        supplier_title = u'Ministère de l’Agriculture, de l’Agroalimentaire et de la Forêt',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'https://fichiers-publics.agriculture.gouv.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    log.info(u'Retrieving list of source packages')
    request_url = urlparse.urljoin(source_site_url, u'etalab/ETALAB/Meta_donnees/')
    request = urllib2.Request(request_url, headers = source_headers)
    response = urllib2.urlopen(request)
    index_tree = lxml.html.fromstring(response.read())
    datasets_filename = [
        a_element.get('href')
        for a_element in index_tree.xpath('//ul/li/a')[1:]  # Skip parent directory.
        ]

    for dataset_filename in datasets_filename:
        request_url = urlparse.urljoin(source_site_url, u'etalab/ETALAB/Meta_donnees/{}'.format(dataset_filename))
        request = urllib2.Request(request_url, headers = source_headers)
        response = urllib2.urlopen(request)
        dataset_tree = etree.parse(response)
        dataset_root_element = convert_xml_element_to_python(dataset_tree.getroot())
        dataset = conv.check(conv.pipe(
            validate_xml_python,
            conv.not_none,
            ))(dataset_root_element, state = conv.default_state)
        metadata = dataset['metadata']

        package = dict(
            author = metadata[u'author'],
            author_email = metadata[u'author_email'],
            extras = metadata[u'extras'],
            frequency = metadata[u'frequency'],
            license_id = metadata[u'license_id'],
            maintainer = metadata[u'maintainer'],
            maintainer_email = metadata[u'maintainer_email'],
            notes = metadata[u'notes'],
            private = metadata[u'private'],
            resources = [
                dict(
                    # created =
                    format = resource[u'format'],
                    # last_modified =
                    name = resource[u'name'],
                    url = resource[u'url'],
                    )
                for resource in metadata[u'resources']
                ],
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(metadata[u'tags']))
                ],
            temporal_coverage_from = metadata[u'temporal_coverage_from'],
            temporal_coverage_to = metadata[u'temporal_coverage_to'],
            territorial_coverage = territorial_coverage_translations.get(metadata[u'territorial_coverage'][
                u'territorial_coverage_code']),
            territorial_coverage_granularity = granularity_translations.get(metadata[u'territorial_coverage'][
                u'territorial_coverage_granularity']),
            title = metadata[u'title'],
            # url =
            )

        log.info(u'Harvested package: {}'.format(package['title']))
        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = group_title,
                    ))
                for group_title in (metadata[u'groups'] or [])
                ]

            organization = harvester.upsert_organization(dict(
                title = organization_title_translations.get(metadata['organization'], metadata['organization']),
                ))

            harvester.add_package(package, organization, metadata[u'id'], request_url, groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
