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


"""Harvest Open Data Soft CKAN repository.

http://www.opendatasoft.com/
"""


import argparse
import base64
import ConfigParser
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(app_name)


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

    if package.get('private', False) or package.get('capacity') == u'private':
        return None, None

#    package = package.copy()

    return package, None


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
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
                'opendatasoft.ckan.password': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'opendatasoft.ckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'opendatasoft.ckan.username': conv.pipe(
                    conv.cleanup_line,
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
        ))(dict(config_parser.items('Etalab-OpenDataSoft-Harvester')), conv.default_state)

    harvester = helpers.Harvester(
        supplier_abbreviation = u'ods',
        supplier_title = u"OpenDataSoft",
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'Authorization': 'Basic {}'.format(base64.encodestring('{}:{}'.format(conf['opendatasoft.ckan.username'],
            conf['opendatasoft.ckan.password'])).replace('\n', '')),
        'User-Agent': conf['user_agent'],
        }
    source_site_url = conf['opendatasoft.ckan.site_url']

    harvester.retrieve_target()

    # Retrieve names of packages in source.
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_list'),
        headers = source_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    packages_source_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    # Retrieve packages from source.
    for package_source_name in packages_source_name:
        request = urllib2.Request(urlparse.urljoin(source_site_url, 'api/3/action/package_show'),
            headers = source_headers)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                id = package_source_name,
                ))))  # CKAN 1.7 requires a POST.
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            before_ckan_json_to_package,
            conv.make_ckan_json_to_package(drop_none_values = True),
            after_ckan_json_to_package,
            ))(response_dict['result'], state = conv.default_state)
        if package is None:
            continue

        publisher = helpers.get_extra(package, 'publisher')
        organization_infos = {
            None: (u"OpenDataSoft", None),
            u"adt-et-ots-des-alpes-de-haute-provence": False,  # Datasets must be merged.
            u"agence-bio": False,  # Direct member of data.gouv.fr
            u"agence-des-espaces-verts-idf": (None, None),
            u"autolib": (None, None),
            u"comite-departemental-de-tourisme-du-pas-de-calais": False,
            u"conseil-general-des-hauts-de-seine": (None, None),
            u"ctc-corse": False,  # Bad titles and descriptions
            u"direction-regionale-du-travail-de-l-emploi-et-de-la-formation-professionnelle": False,  # Direct member of data.gouv.fr?
            u"driea-sit-del-2": (None, None),
            u"federation-nationale-des-bistrots-de-pays": (None, None),
            u"gip-corse-competences": (u"GIP Corse Compétences", None),
            u"iau-idf": (None, None),
            u"ign": False,  # Direct member of data.gouv.fr
            u"insee": False,  # Direct member of data.gouv.fr
            u"jcdecaux-developer": (None, None),
            u"la-poste": False,  # Direct member of data.gouv.fr
            u"le-rif": (None, None),
            u"ministere-de-l-education-nationale": False,  # Direct member of data.gouv.fr
            u"ministere-de-l-interieur": False,  # Direct member of data.gouv.fr
            u"ministere-de-la-culture-et-de-la-communication": False,  # Direct member of data.gouv.fr
            u"ministere-de-la-justice": False,  # Direct member of data.gouv.fr
            u"ministere-des-sports": False,  # Direct member of data.gouv.fr
            u"oehc": False,  # Bad titles and descriptions
            u"premier-ministre-direction-de-l-information-legale-et-administrative": False,  # Direct member of data.gouv.fr
            u"ratp": False,  # Direct member of data.gouv.fr
            u"reseau-ferre-de-france": False,  # Direct member of data.gouv.fr
            u"region-ile-de-france": False,  # Datasets must be merged.
            u"sncf": (u"Société nationale des chemins de fer français", None),  # Direct member of data.gouv.fr, but other datasets
            u"societe-nationale-des-chemins-de-fer-francais": False,  # Direct member of data.gouv.fr
            u"ville-de-paris": (u"Mairie de Paris", None),
            u"ville-de-paris-direction-de-la-proprete-et-de-l-eau": (u"Mairie de Paris",
                u"Direction de la propreté et de l'eau"),
            }.get(strings.slugify(publisher))
        if organization_infos is None:
            log.warning(u'Ignoring package "{}" from unknown publisher "{}"'.format(package['title'], publisher))
            continue
        if organization_infos is False:
            continue
        organization_title, author = organization_infos
        if organization_title is None:
            organization_title = publisher
        organization = harvester.upsert_organization(dict(
            title = organization_title,
            ))

        package['author'] = author
        package.pop('groups', None)
        source_name = package.pop('name')
        package.pop('users', None)

        for resource in package['resources']:
            if resource['format'] == 'HTML':
                source_url = resource['url']
                break
        else:
            TODO
            source_url = u'TODO URL'
        helpers.set_extra(package, u'Source', source_url)
        helpers.pop_extra(package, 'publisher', None)

        package = conv.check(conv.ckan_input_package_to_output_package)(package, state = conv.default_state)
        log.info(u'Harvested package: {}'.format(package['title']))
        harvester.add_package(package, organization, source_name, source_url)

    harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
