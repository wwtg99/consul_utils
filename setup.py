import sys
import os
import io
from setuptools import setup, find_packages

sys.path.insert(0, os.path.abspath('lib'))
from consul_utils import __prog__, __version__, __author__, __author_email__, __descr__


with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()


static_setup_params = dict(
    name=__prog__,
    version=__version__,
    description=__descr__,
    long_description=readme,
    long_description_content_type='text/markdown',
    author=__author__,
    author_email=__author_email__,
    url='https://github.com/wwtg99/consul_utils',
    license='MIT',
    keywords=['consul', 'key_value', 'search', 'diff'],
    python_requires='>=3.5',
    packages=find_packages('lib'),
    package_dir={'': 'lib'},
    install_requires=[
        'hsettings>=0.1',
        'python-consul',
        'click',
        'colorama',
        'diskcache'
    ],
    entry_points = {
        'console_scripts': [
            'consul_utils = consul_utils.application:cli'
        ]
    },
    classifiers=[
        "Operating System :: OS Independent",
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        ],
    # Installing as zip files would break due to references to __file__
    zip_safe=False
)


def main():
    """Invoke installation process using setuptools."""
    setup(**static_setup_params)


if __name__ == '__main__':
    main()
