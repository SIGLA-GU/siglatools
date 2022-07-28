#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

test_requirements = [
    "flake8",
    "black",
    "pytest",
    "pytest-cov",
    "pytest-raises",
]

setup_requirements = [
    "pytest-runner",
]

dev_requirements = [
    "bumpversion>=0.5.3",
    "coverage>=5.0a4",
    "flake8>=3.7.7",
    "furo>=2022.4.7",
    "jinja2>=2.11.2",
    "ipython>=7.5.0",
    "m2r2>=0.2.7",
    "pytest>=4.3.0",
    "pytest-cov==2.6.1",
    "pytest-raises>=0.10",
    "pytest-runner>=4.4",
    "Sphinx>=3.4.3",
    "tox>=3.5.2",
    "twine>=1.13.0",
    "wheel>=0.33.1",
]

interactive_requirements = [
    "altair",
    "jupyterlab",
    "matplotlib",
]

requirements = [
    "google-api-python-client==2.54.0",
    "google-auth==2.9.1",
    "pymongo[tls]==4.2.0",
    "dnspython==2.2.1",
    "dask[bag]==2022.7.1",
    "distributed==2022.7.1",
    "prefect==1.2.4",
    "requests==2.28.1",
    "urllib3==1.26.11",
]

extra_requirements = {
    "test": test_requirements,
    "setup": setup_requirements,
    "dev": dev_requirements,
    "interactive": interactive_requirements,
    "all": [
        *requirements,
        *test_requirements,
        *setup_requirements,
        *dev_requirements,
        *interactive_requirements
    ]
}

setup(
    author="SIGLA",
    author_email="huynh.nto@gmail.com",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
    ],
    description="Tools to extract SIGLA data from Google Sheets and load into MongoDB",
    entry_points={
        "console_scripts": [
            "run_sigla_pipeline=siglatools.bin.run_sigla_pipeline:main",
            "run_external_link_checker=siglatools.bin.run_external_link_checker:main",
            "get_next_uv_dates=siglatools.bin.get_next_uv_dates:main",
            "run_qa_test=siglatools.bin.run_qa_test:main",
            "load_spreadsheets=siglatools.bin.load_spreadsheets:main",
        ],
    },
    install_requires=requirements,
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="siglatools",
    name="siglatools",
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*"]),
    python_requires=">=3.8",
    setup_requires=setup_requirements,
    test_suite="siglatools/tests",
    tests_require=test_requirements,
    extras_require=extra_requirements,
    url="https://github.com/sigla-gu/siglatools",
    # Do not edit this string manually, always use bumpversion
    # Details in CONTRIBUTING.rst
    version="0.1.0",
    zip_safe=False,
)
