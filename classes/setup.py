from pathlib import Path
from pkg_resources import parse_requirements
from setuptools import find_packages, setup


# for path in Path('./').rglob('requirements.txt'):
#     with Path(path).open() as requirements_txt:
#         install_requires = [
#             str(requirement)
#             for requirement
#             in parse_requirements(requirements_txt)
#         ]


config = {
    "description": "Superclasses for blueprints",
    "author": "Shipyard Team",
    "url": "https: // www.shipyardapp.com",
    "author_email": "tech@shipyardapp.com",
    # "packages": ['shipyard-rudderstack-blueprints', ],
    # "install_requires": install_requires,
    # 'py_modules': ['rudderstack'],
    "name": "templates",
    "version": "v0.0.1",
    "license": "Apache-2.0",
    "classifiers": [
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    "python_requires": ">=3.9"}

setup(**config)