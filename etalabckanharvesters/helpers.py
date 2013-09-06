#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-CKAN-Harvesters -- Harvesters for Etalab's CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
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


"""Helpers for harvesters"""


import json
import logging
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings
from ckantoolbox import ckanconv

conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(__name__)


def get_extra(instance, key, default = UnboundLocalError):
    for extra in (instance.get('extras') or []):
        if extra['key'] == key:
            return extra.get('value')
    if default is UnboundLocalError:
        raise KeyError(key)
    return default


def pop_extra(instance, key, default = UnboundLocalError):
    for index, extra in enumerate(instance.get('extras') or []):
        if extra['key'] == key:
            del instance['extras'][index]
            return extra.get('value')
    if default is UnboundLocalError:
        raise KeyError(key)
    return default


def set_extra(instance, key, value):
    if instance.get('extras') is None:
        instance['extras'] = []
    for extra in instance['extras']:
        if extra['key'] == key:
            extra['value'] = value
            return
    instance['extras'].append(dict(
        key = key,
        value = value,
        ))


def upsert_organization(site_url, organization, headers = None):
    assert headers is not None

    organization['name'] = name = strings.slugify(organization['title'])[:100]
    if organization.get('name') is None:
        organization['name'] = name
    else:
        assert organization['name'] == name, organization

    request = urllib2.Request(urlparse.urljoin(site_url,
        'api/3/action/organization_show?id={}'.format(name)), headers = headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as response:
        if response.code != 404:
            raise
        existing_organization = {}
    else:
        response_text = response.read()
        try:
            response_dict = json.loads(response_text)
        except ValueError:
            log.error(u'An exception occured while reading organization: {0}'.format(organization))
            log.error(response_text)
            raise
        existing_organization = conv.check(conv.pipe(
            conv.make_ckan_json_to_organization(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
    organization['packages'] = existing_organization.get('packages') or []
    if existing_organization.get('id') is None:
        # Create organization.
        request = urllib2.Request(urlparse.urljoin(site_url, 'api/3/action/organization_create'),
            headers = headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            log.error(u'An exception occured while creating organization: {0}'.format(organization))
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            created_organization = response_dict['result']
#            pprint.pprint(created_organization)
            organization['id'] = created_organization['id']
    else:
        # Update organization.
        organization['id'] = existing_organization['id']
        organization['state'] = 'active'

        request = urllib2.Request(urlparse.urljoin(site_url, 'api/3/action/organization_update?id={}'.format(name)),
            headers = headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            log.error(u'An exception occured while updating organization: {0}'.format(organization))
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
#            updated_organization = response_dict['result']
#            pprint.pprint(updated_organization)
    return organization


def upsert_package(site_url, package, headers = None):
    assert headers is not None
    package['name'] = name = strings.slugify(package['title'])[:100]

    request = urllib2.Request(urlparse.urljoin(site_url, 'api/3/action/package_show?id={}'.format(name)),
        headers = headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as response:
        if response.code != 404:
            raise
        existing_package = {}
    else:
        response_text = response.read()
        try:
            response_dict = json.loads(response_text)
        except ValueError:
            log.error(u'An exception occured while reading package: {0}'.format(package))
            log.error(response_text)
            raise
        existing_package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
    if existing_package.get('id') is None:
        # Create package.
        request = urllib2.Request(urlparse.urljoin(site_url, 'api/3/action/package_create'),
            headers = headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            log.error(u'An exception occured while creating package: {0}'.format(package))
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            created_package = response_dict['result']
#            pprint.pprint(created_package)
            package['id'] = created_package['id']
    else:
        # Update package.
        package['id'] = existing_package['id']
        package['state'] = 'active'

        request = urllib2.Request(urlparse.urljoin(site_url,
            'api/3/action/package_update?id={}'.format(name)), headers = headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            log.error(u'An exception occured while updating package: {0}'.format(package))
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                log.debug('{} = {}'.format(key, value))
            raise
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
#            updated_package = response_dict['result']
#            pprint.pprint(updated_package)
    return package
