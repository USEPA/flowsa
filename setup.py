"""
Setup for flowsa package
"""

from setuptools import setup, find_packages

setup(
    name='flowsa',
    version='1.3.2',
    packages=find_packages(),
    package_dir={'flowsa': 'flowsa'},
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        'fedelemflowlist @ git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List@v1.1.1#egg=fedelemflowlist',
        'esupy @ git+https://github.com/USEPA/esupy@v0.2.2#egg=esupy',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories@v1.0.6#egg=StEWI',
        'appdirs>=1.4.3',
        'bibtexparser>=1.2.0',
        "kaleido==0.1.0.post1;platform_system=='Windows'",
        "kaleido==0.2.0;platform_system=='Linux' or platform_system=='Darwin'",
        'matplotlib>=3.4.3',
        'numpy>=1.20.1',
        'openpyxl>=3.0.7',
        'pandas>=1.4.0',
        'pip>=9',
        'plotly>=5.10.0 ',
        'pycountry>=19.8.18',
        'python-dotenv >= 0.19.1',
        'pyyaml>=5.3',
        'requests>=2.22.0',
        'requests_ftp==0.3.1',
        'seaborn>=0.11.2',
        'setuptools>=41',
        'tabula-py>=2.1.1',
        'xlrd>=2.0.1'
    ],
    url='https://github.com/USEPA/FLOWSA',
    license='MIT',
    author='Catherine Birney, Ben Young, Matthew Chambers, Melissa Conner, '
           'Jacob Specht, Mo Li, and Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
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
