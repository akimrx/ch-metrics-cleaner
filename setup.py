#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


def requirements():
    requirements_list = []

    with open('requirements.txt') as requirements:
        for install in requirements:
            requirements_list.append(install.strip())

    return requirements_list


requirements = requirements()
packages = find_packages()


def main():
    setup(name='clickhouse-cleaner',
          description='Data cleaner for Clickhouse ',
          version="1.0.0",
          author='Akim Faskhutdinov',
          author_email='akimstrong@yandex.ru',
          platforms=['osx', 'linux'],
          packages=packages,
          include_package_data=True,
          python_requires=">=3.6",
          install_requires=requirements,
          zip_safe=False,
          entry_points={
              "console_scripts": [
                  "clickhouse-cleaner=cleaner.clickhouse_cleaner:main",
              ],
          },
          keywords=['clickhouse', 'cleaner'])


if __name__ == '__main__':
    main()

