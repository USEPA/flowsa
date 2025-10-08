"""
Setup for flowsa package
"""

from setuptools import setup, find_packages

setup(
    name='flowsa',
    version='2.1.0',
    packages=find_packages(),
    package_dir={'flowsa': 'flowsa'},
    include_package_data=True,
    python_requires=">=3.9",
    install_requires=[
        'fedelemflowlist @ git+https://github.com/USEPA/fedelemflowlist.git@develop#egg=fedelemflowlist',
        'esupy @ git+https://github.com/USEPA/esupy.git@develop#egg=esupy',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories.git@develop#egg=StEWI',
        'appdirs>=1.4.4',
        'bibtexparser>=1.4.3',
        "kaleido==0.1.0.post1;platform_system=='Windows'",
        "kaleido==0.2.0;platform_system=='Linux' or platform_system=='Darwin'",
        'matplotlib>=3.10.3',
        'numpy>=2.3.0',
        'openpyxl>=3.1.5',
        'pandas>=2.3.3',
        'pip>=25.1.1',
        'plotly>= 6.1.2',
        'pycountry>=24.6.1',
        'python-dotenv >= 1.1.0',
        'pyyaml>=6.0.2',
        'requests>=2.32.4',
        'requests_ftp==0.3.1',
        'seaborn>=0.13.2',
        'setuptools>=80.9.0',
        'tabula-py>=2.10.0',
        'xlrd>=2.0.1'
    ],
    url='https://github.com/USEPA/FLOWSA',
    license='MIT',
    author='Catherine Birney, Ben Young, Matthew Chambers, and Wesley '
           'Ingwersen',
    author_email='catherine.birney@erg.com',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: IDE",
        "Intended Audience :: Science/Research",
        "License :: MIT",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ],
    description='Attributes resources (environmental, monetary, and human), '
                'emissions, wastes, and losses to US industrial and final '
                'use sectors.'
)
