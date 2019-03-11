from setuptools import setup, find_packages

__version__ = '1.0'

packages = find_packages('.')

# Setup
setup(
    name="gridappsd",
    version=__version__,
    install_requires=['PyYaml', 'stomp.py', 'pytz'],
    packages=packages,
)