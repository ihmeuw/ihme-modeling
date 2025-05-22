import os

from setuptools import setup, find_packages


if __name__ == '__main__':
    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, 'src')

    about = {}
    with open(os.path.join(src_dir, 'covid_gbd_model', '__about__.py')) as f:
        exec(f.read(), about)

    with open(os.path.join(base_dir, 'README.md')) as f:
        long_description = f.read()

    install_requirements = [
        'fastparquet',
        'jobmon[ihme]',
        'loguru',
        'matplotlib',
        'seaborn',
        'pyarrow',
        'tqdm',
        'pandas',
        'numpy',
        'scipy',
        'openpyxl',
        'click',
        'pypdf',

        # 'db-queries',
        # 'save-results',

        'onemod @ git+https://github.com/ihmeuw-msca/OneMod.git@mortality-estimates',
    ]

    test_requirements = []

    doc_requirements = []

    setup(
        name=about['__title__'],
        version=about['__version__'],

        description=about['__summary__'],
        long_description=long_description,
        license=about['__license__'],
        url=about['__url__'],

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
            'dev': [doc_requirements, test_requirements]
        },

        zip_safe=False,
    )
