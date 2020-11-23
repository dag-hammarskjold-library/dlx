version = '1.2.3'

import sys
from setuptools import setup, find_packages

classifiers = """
Intended Audience :: Education
Intended Audience :: Developers
Intended Audience :: Information Technology
License :: OSI Approved :: BSD License
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Topic :: Text Processing :: General
"""

with open("README.md") as f:
    long_description = f.read()
    
with open("requirements.txt") as f:
    requirements = list(filter(None,f.read().split('\n')))

setup(
    name = 'dlx',
    version = version,
    url = 'http://github.com/dag-hammarskjold-library/dlx',
    author = 'United Nations Dag Hammarskjöld Library',
    author_email = 'library-ny@un.org',
    license = 'http://www.opensource.org/licenses/bsd-license.php',
    packages = find_packages(exclude=['tests']),
    package_data = {'dlx': 
        [
            'schemas/jmarc.schema.json',
            'schemas/jfile.schema.json'
        ]
    },
    test_suite = 'tests',
    install_requires = requirements,
    description = 'Read, write and modify DLX data.',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    classifiers = list(filter(None, classifiers.split('\n'))),
    python_requires = '>=3.3',
    entry_points = {
        'console_scripts': [
            'excel-marc=dlx.scripts.excel_marc:run',
            'init-indexes=dlx.scripts.init_indexes:run'
        ]
    }
)
