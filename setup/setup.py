from setuptools import setup

setup(
    name='cherry_pyformance',
    version='0.1.4',
    author = 'Arts Alliance Media',
    author_email='development@artsalliancemedia.com',
    url='http://www.artsalliancemedia.com',
    packages=['cherry_pyformance'],
    data_files=[
        ('cherry_pyformance', ['cherry_pyformance/default_config.cfg'])
    ]
)
