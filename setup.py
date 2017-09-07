# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

test_requirements = [
    'nose',
    'nose-watch'
]

setup(
    name="htetl",
    version="0.0.1",
    description="ETL for human trafficking detection",
    author="anidata",
    author_email="info@anidata.org",
    license="BSD",
    ext_package="htetl",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'luigi',
        'psycopg2',
        'networkx'
    ],
    test_requires=test_requirements,
    extras_require={
        'tests': test_requirements
    }
)
