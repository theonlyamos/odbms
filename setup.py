from importlib.metadata import entry_points
from setuptools import setup, find_packages

VERSION = '0.2.1'

setup(
    name='odbms',
    version=VERSION,
    author='Amos Amissah',
    author_email='theonlyamos@gmail.com',
    description='Database client for Mysql, MongoDB and Sqlite',
    long_description='Connect to all kinds of databases from your application',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['python-dotenv','pymongo', 'mysql', 'mysql-connector', 'mysql-connector-python'],
    keywords='python3 runit developer serverless architecture docker mysql mongodb',
    project_urls={
        'Source': 'https://github.com/theonlyamos/odbms/',
        'Tracker': 'https://github.com/theonlyamos/odbms/issues',
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ]
)
