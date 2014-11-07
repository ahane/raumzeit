try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Time and space based search',
    'author': 'Alec Hanefeld',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'author_email': 'alec@hanefeld.eu',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['raumzeit'],
    'scripts': [],
    'name': 'raumzeit'
}

setup(**config)