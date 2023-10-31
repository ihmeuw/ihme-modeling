from pathlib import Path
from setuptools import setup, find_packages


if __name__ == '__main__':
    base_dir = Path(__file__).parent
    src_dir = base_dir/'src'

    about = {}
    with (src_dir/'mrtool'/'__about__.py').open() as f:
        exec(f.read(), about)

    with (base_dir/'README.rst').open() as f:
        long_description = f.read()

    install_requirements = [
        'numpy',
        'pandas',
        'scipy',
        'xspline',
        'xarray'
    ]

    unsolved_requirements = [
        'ipopt',
        'limetr',
        'pycddlib'
    ]

    test_requirements = [
        'pytest',
        'pytest-mock'
    ]

    doc_requirements = [
        'sphinx>3.0',
        'sphinx-autodoc-typehints',
        'sphinx-rtd-theme',
        'IPython',
        'matplotlib'
    ]

    setup(name=about['__title__'],
          version=about['__version__'],

          description=about['__summary__'],
          long_description=long_description,
          license=about['__license__'],
          url=about['__uri__'],

          author=about['__author__'],
          author_email=about['__email__'],

          package_dir={'': 'src'},
          packages=find_packages(where='src'),
          include_package_data=True,

          install_requires=install_requirements,
          tests_require=test_requirements,
          extras_require={
              'docs': doc_requirements,
              'test': test_requirements,
              'dev': doc_requirements + test_requirements
          },
          zip_safe=False,)
