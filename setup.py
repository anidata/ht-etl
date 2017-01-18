# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from distutils.core import setup

setup(
        name="htetl",
        version="0.0.1",
        description="ETL for human trafficking detection",
        author="anidata",
        author_email="info@anidata.org",
        license="BSD",
        ext_package = "htetl",
        packages=["htetl", "htetl"],
        package_dir={'htetl': 'htetl'},
        install_requires=['pandas',
                          'luigi',
                          'psycopg2',
                          'sqlalchemy',
                          'networkx']
        )
