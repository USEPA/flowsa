"""
Setup for flowsa package
"""

from setuptools import setup, find_packages

setup(
    name='flowsa',
    version='0.4.1',
    packages=find_packages(),
    package_dir={'flowsa': 'flowsa'},
    package_data={'flowsa': ["data/*.*"]},
    include_package_data=True,
    install_requires=[
        'fedelemflowlist @ git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List.git@develop#egg=fedelemflowlist',
        'esupy @ git+https://github.com/USEPA/esupy.git@develop#egg=esupy',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories.git@develop#egg=StEWI',
        'pandas>=1.3.2',
        'pip>=9',
        'setuptools>=41',
        'pyyaml>=5.3',
        'requests>=2.22.0',
        'appdirs>=1.4.3',
        'pycountry>=19.8.18',
        'xlrd>=2.0.1',
        'openpyxl>=3.0.7',
        'requests_ftp==0.3.1',
        'tabula-py>=2.1.1',
        'numpy>=1.20.1',
        'bibtexparser>=1.2.0',
        'joblib >= 1.1.0',
        'python-dotenv >= 0.19.1'
    ],
    url='https://github.com/USEPA/FLOWSA',
    license='CC0',
    author='Catherine Birney, Ben Young, Wesley Ingwersen, Melissa Conner, Jacob Specht, Mo Li',
    author_email='ingwersen.wesley@epa.gov',
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Environment :: IDE",
        "Intended Audience :: Science/Research",
        "License :: CC0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ],
    description='Complies and provides a standardized list of elementary flows and '
                'flow mappings for life cycle assessment data'
)
