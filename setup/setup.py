from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES

for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

setup(name='cherry_pyformance',
      version='0.1',
      packages=['cherry_pyformance'],
      data_files=[('cherry_pyformance', ['cherry_pyformance/default_config.json'])]
     )