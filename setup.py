from importlib.metadata import entry_points
from setuptools import setup, find_packages

VERSION = '0.4.1'

with open('README.md', 'rt') as file:
    description = file.read()

setup(
    name='odbms',
    version=VERSION,
    author='Amos Amissah',
    author_email='theonlyamos@gmail.com',
    description='Database client for Mysql, MongoDB and Sqlite',
    long_description=description,
    packages=find_packages(),
    long_description_content_type = "text/markdown",
    include_package_data=True,
    # install_requires=['python-dotenv','pymongo', 'mysql', 'mysql-connector', 'mysql-connector-python'],
    install_requires=['python-dotenv','pymongo', 'psycopg2-binary', 'inflect', 'pydantic', 'pyreadline3'],
    keywords='python3 runit developer serverless architecture docker sqlite mysql mongodb',
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
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    entry_points={
        'console_scripts': [
            'odbms=odbms.cli:main',
        ],
    },
)
