#!/usr/bin/env python3
from glob import glob
import os.path
import urllib.parse
from collections import defaultdict
import luqum.parser
import luqum.tree

from .utils import merge_list_defaultdicts, clean


def get_lucene_query_tree(query):
    unquoted_query = urllib.parse.unquote_plus(
        urllib.parse.unquote_plus(query))
    return luqum.parser.parser.parse(unquoted_query)


def get_field_to_value_counts_from_query(query):
    try:
        tree = get_lucene_query_tree(query)
    except:
        tree = get_lucene_query_tree(query.replace('[', ' ').replace(']', ''))
    return get_field_to_value_counts_from_query_tree(tree)


def get_field_to_value_counts_from_query_tree(tree):
    field_to_values = defaultdict(list)
    if hasattr(tree, 'name'):
        field = clean(tree.name)
        try:
            value = clean(tree.expr.value)
            field_to_values[field].append(value)
        except:
            field_to_values[field].append(None)
    else:
        # TODO: handle when a node doesn't have a name
        pass
    if tree.children:
        for child in tree.children:
            _field_to_values = get_field_to_value_counts_from_query_tree(child)
            field_to_values = merge_list_defaultdicts(
                _field_to_values, field_to_values)
    return field_to_values
