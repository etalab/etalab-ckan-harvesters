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


"""Harvest open data from "SNCF Open Data".

http://data.sncf.com/
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
    u'Gare': u'poi',
    u'Gare Transilien (Ile-de-France)': u'poi',
    u'Ligne du réseau Transilien (Ile-de-France)': u'poi',
    u'Par gare': u'poi',
    u'Par gare et train TER': u'poi',
    }
license_id_by_license = {
    u'Données Confidentielles': u'other-closed',
    u'SNCF Open Data': u'other-open',
    u"Open Data Commons Open Database License (ODbL)": u'odc-odbl',
    u'Open Database License (ODbL)': u'odc-odbl',
    }
log = logging.getLogger(app_name)
territory_by_tag_name = {
    u'france': u'Country/FR/FRANCE',
    u'ile-de-france': u'RegionOfFrance/11/ILE DE FRANCE',
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
        supplier_abbreviation = u'sncf',
        supplier_title = u'SNCF',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://ressources.data.sncf.com/'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve list of packages in source.
    datasets = opendatasoftcommon.retrieve_datasets(source_headers, source_site_url)

    all_tags_name = set()
    for entry in datasets:
        dataset = conv.check(
            opendatasoftcommon.make_json_to_dataset(
                creators = [
                    u'SNCF Transilien',
                    ],
                domain = u'datasncf',
                granularity_translations = granularity_translations,
                group_title_translations = {
                    u'Comptage et flux': u"Territoires et Transports",
                    u'Equipements et services en gare': u"Territoires et Transports",
                    u"Gares et points d'arrêt": u"Territoires et Transports",
                    u'Horaires et itinéraires': u"Territoires et Transports",
                    u'Qualité de service': u"Territoires et Transports",
                    u'Tarification': u"Territoires et Transports",
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
            publishers_to_ignore = set([
                u"OpenStreetMap",
                ]),
            source_site_url = source_site_url,
            territorial_collectivity = False,
            territorial_coverage = None,
            territory_by_tag_name = territory_by_tag_name,
            )

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Tags: {}'.format(u', '.join(sorted(all_tags_name))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
