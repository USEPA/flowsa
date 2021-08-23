from setuptools import setup

setup(
    name='flowsa',
    version='0.2.1',
    packages=['flowsa'],
    package_dir={'flowsa': 'flowsa'},
    package_data={'flowsa': [
        "data/*.*", "output/*.*"]},
    include_package_data=True,
    install_requires=[
        'fedelemflowlist @ git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List@v1.0.7#egg=fedelemflowlist',
        'esupy @ git+https://github.com/USEPA/esupy@v0.1.7#egg=esupy',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories@v0.9.8#egg=StEWI',
        'pandas>=1.1.0',
        'pip>=9',
        'setuptools>=41',
        'pyyaml>=5.3',
        'requests>=2.22.0',
        'appdirs>=1.4.3',
        'pycountry>=19.8.18',
        'xlrd>=1.2.0',
        'openpyxl>=3.0.7',
        'requests_ftp==0.3.1',
        'tabula-py>=2.1.1',
        'numpy>=1.20.1',
        'bibtexparser>=1.2.0'
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
    description='Complies and provides a standardized list of elementary flows and flow mappings for life cycle assessment data'
)
