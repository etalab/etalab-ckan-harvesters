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


"""Harvest open data from Enseignement supérieur et recherche.

http://data.enseignementsup-recherche.gouv.fr/
"""


import argparse
import ConfigParser
import logging
import os
import sys

from biryani1 import baseconv, custom_conv, states

from . import opendatasoftcommon, helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, states)
granularity_translations = {
    }
license_id_by_license = {
    u'Licence Ouverte (Etalab)': u'fr-lo',
    u'Licence Ouverte/Open Licence': u'fr-lo',
    }
log = logging.getLogger(app_name)
territory_by_tag_name = {
    }


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
                'enseignementsup_recherche.api_key': conv.pipe(
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
        ))(dict(config_parser.items('Etalab-CKAN-Harvesters')), conv.default_state)

    harvester = helpers.Harvester(
        supplier_abbreviation = u'mesr',
        supplier_title = u'Enseignement Supérieur et Recherche',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'https://data.enseignementsup-recherche.gouv.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    datasets = opendatasoftcommon.retrieve_datasets(source_headers, source_site_url,
        api_key = conf['enseignementsup_recherche.api_key'])

    all_tags_name = set()
    for entry in datasets:
        dataset = conv.check(
            opendatasoftcommon.make_json_to_dataset(
                creators = [
#                    u'SNCF Transilien',
                    ],
                domain = u'mesr',
                granularity_translations = granularity_translations,
                group_title_translations = {
                    u'Administration, Gouvernement, Finances publiques, Citoyenneté': u"Société",
                    u'Aménagement du territoire, Urbanisme, Bâtiments, Equipements, Logement':
                        u"Territoires et Transports",
                    u'Economie, Business, PME, Développement économique, Emploi': u"Économie et Emploi",
                    u'Education, Formation, Recherche, Enseignement': u"Éducation et Recherche",
                    },
                license_id_by_license = license_id_by_license,
                temporals = [
                    ],
                ),
            )(entry, state = conv.default_state)
        if dataset is None:
            continue
        metas = dataset['metas']

        tags_name = metas[u'keyword']
        all_tags_name.update(tags_name or [])

        opendatasoftcommon.add_dataset(
            dataset = dataset,
            dry_run = args.dry_run,
            harvester = harvester,
            license_id_by_license = license_id_by_license,
            granularity_translations = granularity_translations,
            publishers_to_ignore = set(),
            source_site_url = source_site_url,
            territorial_collectivity = False,
            territorial_coverage = u'Country/FR/FRANCE',
            territory_by_tag_name = territory_by_tag_name,
            )

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Tags: {}'.format(u', '.join(sorted(all_tags_name))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
