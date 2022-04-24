from setuptools import setup

#This file is called before the main program starts in order to ensure that the necessary non-standard packages required by the program are installed.
setup(name='S20_SE2_DW_Assignment',
      version='1.0',
      description='Survey Data Processing',
      author='Yasmin Al-Azad',
      install_requires=[
          'pandas',
          'pyodbc',
          'cryptography'
      ],
      zip_safe=False)
