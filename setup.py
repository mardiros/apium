import os
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
NAME = 'apium'
with open(os.path.join(here, 'README.rst')) as readme:
    README = readme.read()
with open(os.path.join(here, 'CHANGES.rst')) as changes:
    CHANGES = changes.read()

with open(os.path.join(here, 'apium', '__init__.py')) as version:
    VERSION = re.compile(r".*__version__ = '(.*?)'",
                         re.S).match(version.read()).group(1)


requires = ['zope.interface',
            'PyYAML',
            'simplejson',
            'venusian',
            # git+https://github.com/mardiros/aioamqp.git
            ]

setup(name=NAME,
      version=VERSION,
      description='distributed execution job for asyncio',
      long_description=README + '\n\n' +  CHANGES,
      classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        ],
      author='Guillaume Gauvrit',
      author_email='guillaume@gauvr.it',
      url='',
      keywords='asyncaio distributed job',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      # test_suite=NAME,
      install_requires=requires,
      entry_points="""\
      [console_scripts]
      {pkg} = {pkg}.__main__:main
      """.format(pkg=NAME),
      )
