"""Writes a report ot all auths and their use counts"""

import sys, os
import boto3
from dlx import DB
from dlx.marc import Bib, Auth, BibSet, AuthSet
DB.connect(boto3.client('ssm').get_parameter(Name='prodISSU-admin-connect-string')['Parameter']['Value'], database='undlFiles')

# header
print('\t'.join(['auth#', 'tag', 'heading', 'bib use count', 'auth use count']))

# data
for auth in AuthSet.from_query({}):
    print(
        '\t'.join(
            [
                f'{auth.id}',
                f'{auth.heading_field.tag}',
                ' '.join([f'${x.code} {x.value}' for x in auth.heading_field.subfields]),
                f'{auth.in_use(usage_type="bib")}',
                f'{auth.in_use(usage_type="auth")}'
            ]
        )
    )