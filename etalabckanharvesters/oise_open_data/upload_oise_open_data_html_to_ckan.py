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
import collections
import ConfigParser
import datetime
import itertools
import logging
import os
import re
import sys

from biryani1 import baseconv, custom_conv, datetimeconv, states
from lxml import etree

from .. import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, states)
data_filename_re = re.compile('data-(?P<number>\d+)\.html$')
french_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]\d|3[01]) (?P<month>.+) (?P<year>[12]\d\d\d)')
format_by_image_name = {
    u'doc.png': u'DOC',
    u'pdf.png': u'PDF',
    u'xlsx.png': u'XLS',
    }
frequency_translations = {
    u"Annuelle": u'annuelle',
    u"Chaque BP": None,
    u"chaque BP": None,
    u"Chaque DM1": None,
    u"Chaque DM2": None,
    u"Trimestrielle": u'trimestrielle',
    }
html_parser = etree.HTMLParser()
license_id_by_name = {
    u"Licence Ouverte / Open Licence": u'fr-lo',
    }
log = logging.getLogger(app_name)
name_re = re.compile(u'(\{(?P<url>.+)\})?(?P<name>.+)$')
territorial_coverage_translations = {
    u"Département de l'Oise": u'DepartmentOfFrance/60/60 OISE',
    }
trimester_re = re.compile(ur'T(?P<trimester>[1-4]) (?P<year>\d{4})$')
year_re = re.compile(ur'Année (?P<year>\d{4})$')


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


element_to_str = conv.pipe(
    conv.test_isinstance(dict),
    conv.test(lambda element: len(element) == 2 and set(element.keys()) == set(['@class', '^text'])),
    conv.function(lambda element: element['^text']),
    )


element_to_text = conv.pipe(
    )


def french_input_to_date(value, state = None):
    if value is None:
        return value, None
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
        supplier_abbreviation = u'oise',
        supplier_title = u"Oise Open Data",
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
                # html_base_list = data_html.xpath('head/base[@href]')
                # base_url = html_base_list[0].get('href')

                dataset_html = data_html.xpath('.//div[@class="tx_icsoddatastore_pi1_single"]')[0]
                assert dataset_html is not None

                title_str = dataset_html.xpath('.//h3')[0].text.strip()
                assert title_str

                fields = {}
                for div_html in dataset_html.xpath('.//div[@class="tx_icsoddatastore_pi1_left"]/div'):
                    if div_html.get('class') in (
                            'tx_icsoddatastore_pi1_backlink',
                            'tx_icsoddatastore_pi1_intro separator',
                            ):
                        continue
                    label_html, value_html = div_html.xpath('p')
                    label = label_html.text.strip().rstrip(u':').rstrip()
                    fields[label] = etree.tostring(value_html, encoding = unicode, method = 'text') \
                        if label == u'Description' else convert_xml_element_to_python(value_html)
                entry = conv.check(conv.struct(
                    {
                        u'Contact': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
                            conv.test_in([
                                u"Conseil général de l'Oise",
                                ]),
                            conv.not_none,
                            ),
                        u'Date de création': conv.pipe(
                            element_to_str,
                            french_input_to_date,
                            conv.date_to_iso8601_str,
                            conv.not_none,
                            ),
                        u'Date de mise à jour': conv.pipe(
                            element_to_str,
                            french_input_to_date,
                            conv.date_to_iso8601_str,
                            ),
                        u'Date de sortie': conv.pipe(
                            element_to_str,
                            french_input_to_date,
                            conv.date_to_iso8601_str,
                            conv.not_none,
                            ),
                        u'Description': conv.pipe(
                            conv.cleanup_text,
                            conv.not_none,
                            ),
                        u'Diffuseur': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
                            conv.test_in([
                                u"Conseil général de l'Oise",
                                ]),
                            conv.not_none,
                            ),
                        u'Fréquence de mise à jour': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
                            conv.test_in(frequency_translations),
                            ),
                        u'Identifiant': conv.pipe(
                            element_to_str,
                            conv.input_to_int,
                            conv.not_none,
                            ),
                        u'Licence': conv.pipe(
                            conv.test_isinstance(dict),
                            conv.struct(
                                dict(
                                    a = conv.pipe(
                                        conv.test_isinstance(dict),
                                        conv.struct(
                                            dict(
                                                img = conv.pipe(
                                                    conv.test_isinstance(dict),
                                                    conv.struct(
                                                        {
                                                            u'@alt': conv.pipe(
                                                                conv.cleanup_line,
                                                                conv.test_in(license_id_by_name),
                                                                conv.not_none,
                                                                ),
                                                            },
                                                        default = conv.noop,
                                                        ),
                                                    ),
                                                ),
                                            default = conv.noop,
                                            ),
                                        conv.not_none,
                                        ),
                                    ),
                                default = conv.noop,
                                ),
                            conv.function(lambda element: element['a']['img']['@alt']),
                            conv.not_none,
                            ),
                        u'Mots clés': conv.pipe(
                            element_to_str,
                            conv.function(lambda tags: tags.split(u',')),
                            conv.uniform_sequence(
                                conv.input_to_slug,
                                drop_none_items = True,
                                ),
                            conv.empty_to_none,
                            conv.not_none,
                            ),
                        u'Période de validité': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
#                            conv.test_in([
#                                u"à chaque DOB",
#                                u"01/06/2013",
#                                u"15-12-2012",
#                                ]),
                            ),
                        u'Prérimètre géographique': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
                            conv.test_in(territorial_coverage_translations),
                            conv.not_none,
                            ),
                        u'Propriétaire': conv.pipe(
                            element_to_str,
                            conv.cleanup_line,
                            conv.test_in([
                                u"Conseil général de l'Oise",
                                ]),
                            conv.not_none,
                            ),
                        u'Thématiques': conv.pipe(
                            element_to_str,
                            conv.function(lambda theme: theme.split(u',')),
                            conv.uniform_sequence(
                                conv.cleanup_line,
                                drop_none_items = True,
                                ),
                            conv.empty_to_none,
                            conv.not_none,
                            ),
                        },
                    ))(fields, state = conv.default_state)

                resources = []
                for a_html in dataset_html.xpath('.//div[@class="tx_icsoddatastore_pi1_right"]'
                        '//div[@class="tx_icsoddatastore_pi1_file"]/a'):
                    image_name = a_html.find('img').get('src').rsplit('/', 1)[-1]
                    assert image_name in format_by_image_name, 'Unknown format for {}'.format(image_name)
                    url = a_html.get('href')
                    resources.append(dict(
                        created = entry[u'Date de création'],
                        format = format_by_image_name[image_name],
                        last_modified = entry[u'Date de mise à jour'],
                        name = url.rsplit('/', 1)[-1],
                        url = url,
                        ))
            except:
                print 'An exception occured in file {0}'.format(data_number)
                raise

        package = dict(
            frequency = frequency_translations.get(entry[u'Fréquence de mise à jour']),
            license_id = license_id_by_name[entry[u'Licence']],
            notes = entry[u'Description'],
            resources = resources,
            tags = [
                dict(name = tag_name)
                for tag_name in sorted(set(entry[u'Mots clés'] or []))
                ],
            territorial_coverage = territorial_coverage_translations[entry[u'Prérimètre géographique']],
            title = title_str,
            url = u'http://od.oise-preprod.oxyd.net/index.php?id=38&tx_icsoddatastore_pi1[uid]={}'
                u'&tx_icsoddatastore_pi1[returnID]=38'.format(data_number),
            )

        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = theme,
                    ))
                for theme in entry[u'Thématiques']
                ]
            organization = harvester.upsert_organization(dict(
                title = entry[u'Propriétaire'],
                ))

            harvester.add_package(package, organization, package['title'], package['url'], groups = groups)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
