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


"""Harvest "Le Grand Lyon" CSW repository.

http://catalogue.data.grandlyon.com/

See also: http://smartdata.grandlyon.com/
"""


import argparse
import ConfigParser
import logging
import os
import sys

from biryani1 import baseconv, custom_conv, states, strings
from lxml import etree
from owslib.csw import CatalogueServiceWeb, namespaces
import owslib.iso

from . import helpers


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, states)
log = logging.getLogger(app_name)
namespaces = namespaces.copy()
for key, value in owslib.iso.namespaces.iteritems():
    if key is not None:
        namespaces[key] = value


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
        supplier_abbreviation = u'gl',
        supplier_title = u"Grand Lyon",
        target_headers = {
            'Authorization': conf['ckan.api_key'],
            'User-Agent': conf['user_agent'],
            },
        target_site_url = conf['ckan.site_url'],
        )
    source_site_url = u'http://catalogue.data.grandlyon.com/geosource/srv/fr/csw'

    if not args.dry_run:
        harvester.retrieve_target()

    # Retrieve short infos of packages in source.
    csw = CatalogueServiceWeb(source_site_url)

    bad_indexes = []
    index = 0
    limit = 50
    record_by_id = {}
    while True:
        try:
            csw.getrecords(maxrecords = limit, startposition = index)
        except:
            if limit == 1:
                # Bad record found. Skip it.
                bad_indexes.append(index)
                index += 1
                limit = 50
            else:
                # Retry one by one to find bad record and skip it.
                limit = 1
        else:
            for id, record in csw.records.iteritems():
                record_by_id[id] = record
            next_index = csw.results['nextrecord']
            if next_index <= index:
                break
            index = next_index

    # Retrieve packages from source.
    formats = set()
    licenses_url = set()
    protocols = set()
    rights = set()
    temporals = set()
    types = set()
    for record_id in record_by_id.iterkeys():
        csw.getrecordbyid(id = [record_id])
        dc_record = csw.records[record_id]
        csw.getrecordbyid(id = [record_id], outputschema = 'http://www.isotc211.org/2005/gmd')
        gmd_record = csw.records.get(record_id)

        format = dc_record.format
        if format is not None:
            format = format.split(u' (', 1)[0]
        formats.add(format)

        copyright = dc_record.rights
        if copyright and isinstance(copyright, list):
            copyright = tuple(copyright)
            rights.add(copyright)

        if gmd_record is None:
            frequency = None
        else:
            for frequency_xml in etree.fromstring(gmd_record.xml).xpath('./gmd:identificationInfo'
                    '/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation'
                    '/gmd:userDefinedMaintenanceFrequency/gts:TM_PeriodDuration',
                    namespaces = namespaces):
                frequency = frequency_xml.text
                break
            else:
                frequency = None
        if frequency is not None:
            assert frequency in frequency_by_code, 'Unknown frequency: {}'.format(frequency)
            frequency = frequency_by_code[frequency]

        for uri in dc_record.uris:
            if uri['url'].startswith('http://opendata.data.grandlyon.com/Licence'):
                licenses_url.add(uri['url'])
            protocols.add(uri['protocol'])

        subjects = [
            subject
            for subject in dc_record.subjects
            if subject != 'OpenData'
            ]
        groups = [
            harvester.upsert_group(dict(
                title = subjects[0],
                )),
            ] if subjects else []
        groups.append(harvester.upsert_group(dict(
            title = u'Territoires et Transports',
            )))
        tags = [
            dict(name = strings.slugify(subject))
            for subject in subjects
            ]

        related = []
        if gmd_record is None:
            resources = [
                dict(
                    description = uri.get('description') or None,
                    format = {
                        'application/pdf': 'PDF',
                        'application/zip': 'ZIP',
                        'pdf': 'PDF',
                        'text/csv': 'CSV',
                        'text/plain': 'TXT',
                        }.get(format, format),
                    name = uri.get('name') or None,
                    url = uri['url'],
                    )
                for uri in dc_record.uris
                if uri.get('protocol') in ('WWW:DOWNLOAD-1.0-http--download', 'WWW:LINK-1.0-http--link')
                    and uri['url'].startswith('http://opendata.data.grandlyon.com/')
                    and uri['url'] != 'http://opendata.data.grandlyon.com/Licence_ODbL_Grand_Lyon.pdf'
                ]
        else:
            kml_resource = False
            resources = []
            for online in gmd_record.distribution.online:
                if online.url.startswith((
                        'http://catalogue.data.grandlyon.com/geosource/srv/en/resources.get?id=',
                        'file:',
                        'jdbc:',
                        )) \
                        or online.url == 'http://opendata.data.grandlyon.com/Licence_ODbL_Grand_Lyon.pdf':
                    continue
                if online.protocol == 'OGC:WFS':
                    if not kml_resource:
                        resources.append(dict(
                            description = online.description or None,
                            format = 'KML',
                            name = online.name or None,
                            url = 'http://kml.data.grandlyon.com/grandlyon/?request=list&typename={}'.format(
                                online.name),
                            ))
                        kml_resource = True
                    if '?' not in online.url:
                        resources.append(dict(
                            description = online.description or None,
                            format = 'GML',
                            name = online.name or None,
                            url = u'{}?SERVICE={}&REQUEST=GetFeature&VERSION=1.1.0&typename={}'.format(online.url,
                                online.protocol.split(':', 1)[1], online.name),
                            ))
                elif online.protocol == 'OGC:WMS':
                    if '?' not in online.url:
                        bounding_box = gmd_record.identification.extent.boundingBox
                        related.append(dict(
                            image_url = u'{}?SERVICE={}&REQUEST=GetMap&VERSION=1.1.1&LAYERS={}&FORMAT=image/png'
                                u'&SRS=EPSG:4326&BBOX={},{},{},{}&WIDTH=400&HEIGHT=300'.format(online.url,
                                online.protocol.split(':', 1)[1], online.name, bounding_box.minx, bounding_box.miny,
                                bounding_box.maxx, bounding_box.maxy),
                            title = u'Vignette',
                            type = u'visualization',
                            # url = None,
                            ))
                resources.append(dict(
                    description = online.description or None,
                    format = {
                        'DB:POSTGIS': 'POSTGIS',
                        'FILE:RASTER': 'RASTER',
                        'OGC:WCS': 'WCS',
                        'OGC:WFS': 'WFS',
                        'OGC:WMS': 'WMS',
                        'WWW:DOWNLOAD-1.0-http--download': None,
                        'WWW:LINK-1.0-http--link': None,
                        }[online.protocol],
                    name = online.name or None,
                    url = online.url,
                    ))
        temporals.add(dc_record.temporal)
        types.add(dc_record.type)

        if args.dry_run:
            log.info(u'Harvested package: {}'.format(dc_record.title))
        else:
            package = dict(
                frequency = {
                    'P0Y0M0DT0H1M0S': u"ponctuelle",
                    }.get(frequency),
                license_id = {
                    'copyright': None,
                    ('Licence ODbL GRAND LYON', u"Pas de restriction d'accès public"): u'odc-odbl',
                    'license': None,
                    }.get(copyright),
                notes = u'\n\n'.join(
                    fragment
                    for fragment in (
                        dc_record.abstract,
                        dc_record.source,
                        )
                    if fragment
                    ),
                resources = resources,
                tags = [
                    dict(name = strings.slugify(subject))
                    for subject in dc_record.subjects
                    ],
#                territorial_coverage = TODO
                title = dc_record.title,
#                TODO: Use this URL once Grand Lyon is ready to use it. Before end of year.
#                url = u'http://smartdata.grandlyon.com/single/{}'.format(record_id),
                url = u'http://smartdata.grandlyon.com/',
                )

#            if gmd_record is not None:
#                for graphic_filename_xml in etree.fromstring(gmd_record.xml).xpath('./gmd:identificationInfo'
#                        '/gmd:MD_DataIdentification/gmd:graphicOverview'
#                        '/gmd:MD_BrowseGraphic[gmd:fileDescription/gco:CharacterString="large_thumbnail"]'
#                        '/gmd:fileName/gco:CharacterString',
#                        namespaces = namespaces):
#                    related.append(dict(
#                        image_url = urlparse.urljoin(base_url, unicode(graphic_filename_xml.text)),
#                        title = u'Vignette',
#                        type = u'visualization',
#                        # url = TODO,
#                        ))

            log.info(u'Harvested package: {}'.format(package['title']))
            harvester.add_package(package, harvester.supplier, dc_record.title, package['url'],
                related = related or None)

    if not args.dry_run:
        harvester.update_target()

    log.info(u'Formats: {}'.format(sorted(formats)))
    log.info(u'Licenses: {}'.format(sorted(licenses_url)))
    log.info(u'Protocols: {}'.format(sorted(protocols)))
    log.info(u'Rights: {}'.format(sorted(rights)))
    log.info(u'Temporals: {}'.format(sorted(temporals)))
    log.info(u'Types: {}'.format(sorted(types)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
