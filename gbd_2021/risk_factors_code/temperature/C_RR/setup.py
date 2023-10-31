from setuptools import setup, find_packages


setup(name='temperature',
      version='0.0.0',
      description='Survival analysis.',
      url='https://github.com/ihmeuw-msca/Temperature',
      author='ihmeuw-msca',
      author_email='ihme.math.sciences@gmail.com',
      license='MIT',
      package_dir={'': 'src'},
      packages=find_packages(where='src'),
      install_requires=['numpy',
                        'scipy',
                        'matplotlib',
                        'xspline',
                        'pytest'],
      zip_safe=False)
