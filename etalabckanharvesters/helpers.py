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

