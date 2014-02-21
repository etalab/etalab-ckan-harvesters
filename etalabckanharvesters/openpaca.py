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


"""Harvest open data from "Open PACA".

http://opendata.regionpaca.fr/
"""


import argparse
import ConfigParser
import csv
import datetime
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, states, strings
from lxml import etree

from . import helpers

app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, datetimeconv, states)
french_date_re = re.compile(ur'(?P<day>0?[1-9]|[12]?\d|3[01])/(?P<month>0?[1-9]|1[0-2])/(?P<year>[12]\d\d\d)')
html_parser = etree.HTMLParser()
license_id_by_title = {
    u'Licence CC-BY 3.0': u'cc-by',
    u'Licence CC-BY-SA 2.0': u'cc-by-sa',
    u"Licence Nice Côte d'Azur": u'other-open',
    u'Licence Open Data SNCF': u'other-open',
    u'Licence Ouverte': u'fr-lo',
    u'Licence ODBL': u'odc-odbl',
    }
log = logging.getLogger(app_name)
N_ = lambda message: message
name_re = re.compile(u'(\{(?P<url>.+)\})?(?P<name>.+)$')
organization_title_translations = {
    u'OpenStreetMap et contributeurs': u'OpenStreetMap',
    }


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
        supplier_abbreviation = u'op',
        supplier_title = u'Open PACA',
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_headers = {
        'User-Agent': conf['user_agent'],
        }
    source_site_url = u'http://opendata.regionpaca.fr/'

    if not args.dry_run:
        harvester.retrieve_target()

    log.info(u'Retrieving list of source partners')
    partner_by_title = {}
    for url_path in (
            'partenaires.html',
            'partenaires/page/2.html',
            ):
        request = urllib2.Request(urlparse.urljoin(source_site_url, url_path), headers = source_headers)
        response = urllib2.urlopen(request)
        partners_html = etree.fromstring(response.read(), html_parser)
        for partner_content_html in partners_html.xpath(u'.//div[@class="partner_content"]'):
            partner_image_url_path = partner_content_html.find(u'.//div[@class="partner_image"]/img').get('src')
            assert partner_image_url_path is not None
            partner_text_html = partner_content_html.find(u'.//div[@class="partner_text"]')
            partner_title = partner_text_html.findtext('h2/a').strip() or None
            assert partner_title is not None
            partner_description_html = partner_text_html.find('p')
            if partner_description_html is None:
                assert len(partner_text_html) == 2
                comment_html = partner_text_html[1]
                assert comment_html.tag is etree.Comment
                partner_description = comment_html.tail.strip() or None
                assert partner_description is not None
            else:
                partner_description = etree.tostring(partner_description_html, encoding = 'utf-8',
                    method = 'text').strip() or None
            partner_by_title[partner_title] = dict(
                description = partner_description,
                title = partner_title,
                image_url = urlparse.urljoin(source_site_url, partner_image_url_path),
                )

    # Retrieve short infos of packages in source.
    log.info(u'Retrieving list of source datasets')
    request = urllib2.Request(urlparse.urljoin(source_site_url, 'donnees.html?type=110&no_cache=1'
        '&tx_ausyopendata_pi1%5Bfile%5D=fileadmin%2F_temp_%2Fcatalog.csv'
        '&tx_ausyopendata_pi1%5Baction%5D=export&tx_ausyopendata_pi1%5Bcontroller%5D=Dataset'
        '&cHash=50af02984e8949aa84fa1d9f2acdc4ba'), headers = source_headers)
    response = urllib2.urlopen(request)
    datasets_csv_reader = csv.reader(response, delimiter = ';', quotechar = '"')
    while True:
        labels = datasets_csv_reader.next()
        if not labels or len(labels) == 1 and not labels[0].strip():
            continue
        break
    records = []
    for row in datasets_csv_reader:
        if not row or len(row) == 1 and not row[0].strip():
            continue
        record = dict(
            (label.decode('cp1252'), cell.decode('cp1252'))
            for label, cell in zip(labels, row)
            )
        record = conv.check(conv.struct(
            {
                u'Catégories': conv.pipe(
                    conv.function(lambda s: s.split(u',')),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        ),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                u"Conditions d'utilisation": conv.pipe(
                    conv.cleanup_line,
                    conv.test_in(license_id_by_title),
                    conv.not_none,
                    ),
                u'Couverture géographique': conv.pipe(
                    conv.cleanup_line,
                    conv.test_in([
                        u'Autre',
                        u'Communale',
                        u'Départementale',
                        u'Européenne',
                        u'Intercommunale',
                        u'Internationale',
                        u'Nationale',
                        u'Régionale',
                        ]),
                    conv.not_none,
                    ),
                u'Date de création de la donnée': conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    conv.not_none,
                    ),
                u'Date de dernière mise à jour publiée': conv.pipe(
                    french_input_to_date,
                    conv.date_to_iso8601_str,
                    conv.not_none,
                    ),
                u'Description': conv.pipe(
                    conv.cleanup_text,
                    conv.not_none,
                    ),
                u'Formats': conv.pipe(
                    conv.function(lambda s: s.split(u',')),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.cleanup_line,
                            conv.test(lambda format: format in (format.upper(), u'Autre'),
                                error = N_(u"Invalid format")),
                            conv.not_none,
                            ),
                        ),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                u'Fréquence de mises à jour': conv.pipe(
                    conv.cleanup_line,
                    conv.test_in([
                        u'Annuelle',
                        u'Aucune',
                        u'Hebdomadaire',
                        u'Mensuelle',
                        u'Ponctuelle',
                        u'Quotidienne',
                        u'Semestrielle',
                        u'Triennale',
                        u'Trimestrielle',
                        ]),
                    conv.not_none,
                    ),
                u'Langues': conv.pipe(
                    conv.function(lambda s: s.split(u',')),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.cleanup_line,
                            conv.test_in([
                                u'Allemand',
                                u'Anglais',
                                u'Français',
                                u'Espagnol',
                                u'Italien',
                                u'Néerlandais',
                                ]),
                            conv.not_none,
                            ),
                        ),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                u'Mots clés': conv.pipe(
                    conv.function(lambda s: s.split(u',')),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.cleanup_line,
                            conv.not_none,
                            ),
                        ),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                u'Producteur': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                u'Propriétaire': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                u'Titre': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                u'URL': conv.pipe(
                    conv.make_input_to_url(full = True, error_if_fragment = True),
                    conv.not_none,
                    ),
                },
            default = conv.noop,
            ))(record, state = conv.default_state)
        records.append(record)

    # Retrieve packages from source.
    for record in records:
        if strings.slugify(record[u'Producteur']) in (
                u'atout-france',  # direct data.gouv.fr subscriber
                u'bouches-du-rhone-tourisme',  # direct data.gouv.fr subscriber
                u'etat',
                u'ign',  # direct data.gouv.fr subscriber
                u'reseau-ferre-de-france',  # direct data.gouv.fr subscriber
                u'sncf',  # direct data.gouv.fr subscriber
                ):
            continue
        log.info(u'Harvesting package: {}'.format(record['Titre']))
        request = urllib2.Request(record['URL'], headers = source_headers)
        response = urllib2.urlopen(request)
        data_html = etree.fromstring(response.read(), html_parser)
        base_url = unicode(data_html.xpath('head/base[@href]')[0].get('href'))

        associated_documents = [
            dict(
                created = record[u'Date de création de la donnée'],
                last_modified = record[u'Date de dernière mise à jour publiée'],
                name = unicode(a_html.text),
                url = urlparse.urljoin(base_url, unicode(a_html.get('href'))),
                )
            for a_html in data_html.xpath(u'.//div[text() = "Documents associés :"]/following-sibling::div//a')
            ]

        external_links = [
            dict(
                created = record[u'Date de création de la donnée'],
                last_modified = record[u'Date de dernière mise à jour publiée'],
                name = unicode(a_html.text),
                url = unicode(a_html.get('href')),
                )
            for a_html in data_html.xpath(u'.//div[text() = "Liens externes :"]/following-sibling::div//a')
            ]

        download_links = [
            dict(
                created = record[u'Date de création de la donnée'],
                last_modified = record[u'Date de dernière mise à jour publiée'],
                format = unicode(img_html.get('src')).rsplit(u'.', 1)[0].rsplit(u'-', 1)[-1],
                name = unicode(img_html.get('title')),
                url = urlparse.urljoin(base_url, unicode(a_html.get('href'))),
                )
            for a_html in data_html.xpath(u'.//a[@class = "download_link"]')
            for img_html in a_html.xpath(u'img')
            ]
        if not download_links:
            download_links = [
                dict(
                    created = record[u'Date de création de la donnée'],
                    description = u'Page web proposant de télécharger les données aux formats {}'.format(
                        u', '.join(record['Formats'])),
                    format = 'HTML',
                    last_modified = record[u'Date de dernière mise à jour publiée'],
                    name = a_html.text.strip(),
                    url = urlparse.urljoin(base_url, unicode(a_html.get('href'))),
                    )
                for a_html in data_html.xpath(u'.//h4[text() = "Téléchargez la donnée"]/following-sibling::div[1]//a')
                ]
        assert download_links, u'Record has no data: {}'.format(record).encode('utf-8')

        applications = []
        for any_html in data_html.xpath(
                u'.//h4[text() = "Applications utilisant cette donnée"]/following-sibling::*'):
            if any_html.tag != 'div' or any_html.get('class') != 'picto-item':
                break
            for a_html in any_html.xpath(u'.//a'):
                for img_html in a_html.xpath(u'img'):
                    applications.append(dict(
                        image_url = urlparse.urljoin(base_url, unicode(img_html.get('src'))),
                        title = unicode(img_html.get('title')),
                        type = u'application',
                        url = urlparse.urljoin(base_url, unicode(a_html.get('href'))),
                        ))

        if not args.dry_run:
            groups = [
                harvester.upsert_group(dict(
                    title = {
                        # "Marseille-Provence 2013" is also an organization. And CKAN doesn't allow a group and an
                        # organization to share the same name.
                        u'Marseille-Provence 2013': u'Marseille-Provence 2013 - Capitale européenne de la culture',
                        }.get(category, category),
                    ))
                for category in sorted(set(record[u'Catégories'] + [u"Territoires et Transports"]))
                ]
        license_id = license_id_by_title[record[u"Conditions d'utilisation"]]
# Code commented out to ensure that existing logo and description will not be overridden.
#        partner = partner_by_title.get(organization_title_translations.get(record[u'Producteur'],
#            record[u'Producteur']))
#        if partner is None:
#            partner = dict(title = organization_title_translations.get(record[u'Producteur'],
#                record[u'Producteur']))
        partner = dict(title = organization_title_translations.get(record[u'Producteur'],
            record[u'Producteur']))
        if not args.dry_run:
            organization = harvester.upsert_organization(partner)
        territorial_coverage = {
            u'Européenne': u'InternationalOrganization/UE/UNION EUROPEENNE',
            u'Internationale': u'InternationalOrganization/WW/MONDE',
            u'Nationale': u'Country/FR/FRANCE',
            u'Régionale': u'RegionOfFrance/93/PROVENCE ALPES COTE D AZUR',
            }.get(record[u'Couverture géographique'])
        if territorial_coverage is None:
            territorial_coverage = {
                (u"Arles Crau Camargue Montagnette", u'Intercommunale'):
                    u'IntercommunalityOfFrance/241300417/CA ARLES CRAU CAMARGUE MONTAGNETTE',
                (u"Communauté du Pays d'Aix", u'Intercommunale'):
                    u'IntercommunalityOfFrance/241300276/CA DU PAYS D AIX EN PROVENCE',
                (u"Marseille Provence Métropole", u'Intercommunale'):
                    u'IntercommunalityOfFrance/241300391/CU MARSEILLE PROVENCE METROPOLE',
                (u"Métropole Nice Côte d'Azur", u'Intercommunale'):
                    u'IntercommunalityOfFrance/200030195/METROPOLE NICE COTE D AZUR',
                (u"Pays d'Aubagne et de l'Etoile", u'Intercommunale'):
                    u'IntercommunalityOfFrance/241300268/CA DU PAYS D AUBAGNE ET DE L ETOILE',
                (u"SAN Ouest Provence", u'Intercommunale'): u'IntercommunalityOfFrance/241300177/SAN OUEST PROVENCE',
                (u"Ville d'Aix-en-Provence", u'Communale'): u'CommuneOfFrance/13001/13090 AIX EN PROVENCE',
                (u"Ville de Digne-les-Bains", u'Communale'): u'CommuneOfFrance/04070/04000 DIGNE LES BAINS',
                (u"Ville de Marseille", u'Communale'): u'CommuneOfFrance/13055/13000 MARSEILLE',
                }.get((record[u'Producteur'], record[u'Couverture géographique']))
        package = dict(
            frequency = record[u'Fréquence de mises à jour'].lower(),
            license_id = license_id,
            notes = record[u'Description'],
            resources = download_links + associated_documents + external_links,
            tags = [
                dict(name = strings.slugify(tag_name))
                for tag_name in record[u'Mots clés']
                ],
            territorial_coverage = territorial_coverage,
            title = record[u'Titre'],
            url = record[u'URL'],
            )
        if license_id.startswith('other-'):
            helpers.set_extra(package, u"Conditions d'utilisation", record[u"Conditions d'utilisation"])
        if territorial_coverage is None:
            helpers.set_extra(package, u"Couverture géographique", record[u"Couverture géographique"])
        if record[u"Langues"] != [u'Français']:
            helpers.set_extra(package, u'Langues', u', '.join(record[u"Langues"]))
        helpers.set_extra(package, u'Propriétaire', record[u"Propriétaire"])

        if not args.dry_run:
            harvester.add_package(package, organization, record[u'URL'].rsplit(u'/', 1)[-1].split(u'.', 1)[0],
                record[u'URL'], groups = groups, related = applications)

    if not args.dry_run:
        harvester.update_target()

    return 0


if __name__ == '__main__':
    sys.exit(main())
