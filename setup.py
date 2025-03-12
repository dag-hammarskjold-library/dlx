version = '1.4.22'

import sys
from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()
    
with open("requirements.txt") as f:
    requirements = list(filter(None,f.read().split('\n')))

setup(
    name = 'dlx',
    description = 'Read, write and modify DLX data.',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    version = version,
    url = 'http://github.com/dag-hammarskjold-library/dlx',
    author = 'United Nations Dag Hammarskjöld Library',
    author_email = 'library-ny@un.org',
    license = 'http://www.opensource.org/licenses/bsd-license.php',
    packages = find_packages(exclude=['tests']),
    package_data = {'dlx': ['schemas/jmarc.schema.json', 'schemas/jfile.schema.json']},
    test_suite = 'tests',
    install_requires = requirements,
    python_requires = '>=3.9',
    entry_points = {
        'console_scripts': [
            'excel-marc=dlx.scripts.excel_marc:run',
            'clear-incrementers=dlx.scripts.clear_incrementers:run',
            'init-indexes=dlx.scripts.init_indexes:run',
            'build-logical-fields=dlx.scripts.build_logical_fields:run',
            'build-text-collections=dlx.scripts.build_text_collections:run',
            'auth-merge=dlx.scripts.auth_merge:run',
            'marc-import=dlx.scripts.marc_import:run'
        ]
    }
)
