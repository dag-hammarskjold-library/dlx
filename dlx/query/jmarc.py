"""
Functions for building PyMongo queries for the `bibs` and `auths` collections. 
All functions return BSON objects suitable for use in PyMongo queries. 
The returned objects can be embedded in dicts in order to compose more 
complex queries.
"""

import re
from bson import SON
from dlx.db import DB
from dlx.config import Configs

### subfield matchers
    
def _match_subfield_value(code,val):
    return SON (
        data = {
            '$elemMatch' : {
                'code' : code,
                'value' : val
            }
        }
    )
    
def _match_subfield_xref(code,xref):
    return SON (
        data = {
            '$elemMatch' : {
                'code' : code,
                'xref' : int(xref)
            }
        }
    )
    
def _match_subfield_xrefs(code,*xrefs):
    return SON (
        data = {
            '$elemMatch' : {
                'code' : code,
                'xref' : {
                    '$in' : [*xrefs]
                }
            }
        }
    )
    
### record matchers

def match_value(tag,code,val):
    """Builds a query document for matching a single value within a record. 
    
    Parameters
    ----------
    tag : str
        The field tag to match.
    code : str / None
        The subfield code to match. Use None as the code if matching a controlfield value.
    val : str / Pattern
        Accepts aribtrary number of values to match against. Exact string or a compiled Pattern.
    
    Returns
    -------
    bson.son.SON 
    
    Examples
    -------
    >>> query = match_value('269', 'a', '1999')
    """
    
    valtype = val.__class__.__name__
    
    if code is None:
        return SON (
            data = {
                tag : val
            }
        )
    
    auth_tag = _auth_controlled(tag,code)
    
    if auth_tag is None:
        return SON (
            data = {
                tag + '.subfields' : _match_subfield_value(code,val)
            }
        )
    else:    
        xrefs = _get_xrefs(auth_tag,code,val)
        
        return match_xrefs(tag,code,*xrefs)

def and_values(*tuples):
    return SON (
        data = {
            '$and' : [match_value(*t) for t in tuples]
        }
    )

# provisional                
def or_values(*tuples):
    return SON (
        data = {
            '$or' : [match_value(*t) for t in tuples]
        }
    )
    
def match_field(tag,*tuples,**kwargs):
    """Builds a query document for matching multiple subfield values within a single field in a record. 
    
    Parameters
    ----------
    tag : str
        The field tag to match.
    *tuples : (code [str], val [str / Pattern])
        Accepts arbitrary number of tuples composed of the code and value to 
        match against. Value can be a str or Pattern.
    
        Use `None` as the subfield code if the field is a controlfield.
    
    Returns
    -------
    bson.son.SON 
    
    Examples
    -------
    >>> query = match_field('191', ('b','A/'), ('c','73'))
    """
    
    conditions = []
        
    for t in tuples:
        code = t[0]
        val = t[1]
        auth_tag = _auth_controlled(tag,code)
            
        if auth_tag is None:
            conditions.append(_match_subfield_value(code,val))
        else:
            xrefs = _get_xrefs(auth_tag,code,val)
            conditions.append(_match_subfield_xrefs(code,*xrefs))
    
    if 'modifier' in kwargs.keys():
        if kwargs['modifier'].lower() == 'not':
            return SON(
                data = {
                    '$or': [ 
                        {
                            tag: {
                                '$not': {
                                    '$elemMatch' : {
                                        'subfields' : {
                                            '$all' : conditions
                                        }
                                    }
                                }
                            }
                        },
                        {
                            tag: {'$exists': False}
                        }
                    ]
                }
            )
        elif kwargs['modifier'].lower() == 'exists': 
            return {tag: {'$exists': True}}
        elif kwargs['modifier'].lower() == 'not_exists': 
            return {tag: {'$exists': False}}
    
    return SON(
        data = {
            tag : {
                '$elemMatch' : {
                    'subfields' : {
                        '$all' : conditions
                    }
                }
            }
        }
    )

def and_fields(*tuples):
    field_matchers = _field_matchers(*tuples)
        
    return SON (
        data = {
            '$and' : field_matchers
        }
    )
    
def or_fields(*tuples):
    field_matchers = _field_matchers(*tuples)
            
    return SON (
        data = {
            '$or' : field_matchers
        }
    )
    
def _field_matchers(*tuples):
    field_matchers = []
        
    for t in tuples:
        tag = t[0]
        pairs = t[1:]
        field_matchers.append(match_field(tag,*pairs))
        
    return field_matchers
    
# provisional
def _match_tag(tag):
    return SON(data={'tag' : {'$exists' : True}})

# provisional    
def _match_tag_code(tag,code):
    return SON(data={'tag.subfields' : {'code' : code}})

def match_xrefs(tag,code,*xrefs):
    """Builds a query document for matching records against a list of xrefs.
    
    Parameters
    ----------
    tag : str
        The field tag to match.
    code : str
        The subfield code to match.
    *xrefs : int
        Xrefs to match against.
            
    Returns
    -------
    bson.son.BSON
        
    Examples
    --------
    >>> query = match_xrefs('650','a',268584,274431)
    """
    
    return SON (
        data = {
            tag + '.subfields' : _match_subfield_xrefs(code,*xrefs)
        }
    )
    
def _get_xrefs(tag,code,val):
    cur = DB.auths.find(match_value(tag,code,val),{'_id':1})
    
    ret_vals = []
    
    for doc in cur:
        ret_vals.append(doc['_id'])
        
    return ret_vals
    
def _auth_controlled(tag,code):
    try:
        return Configs.authority_controlled[tag][code]
    except:
        return None
