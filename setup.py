from setuptools import setup

setup(name='synapsebridgehelpers',
      version='0.1',
      description='Convenience functions and code for dealing with bridge data in Synapse',
      url='https://github.com/Sage-Bionetworks/synapsebridgehelpers',
      author='Larson Omberg and Yooree Chae',
      author_email='platform@sagebase.org',
      license='Apache',
      packages=['synapsebridgehelpers'],
      install_requires=[
          'synapseclient',
          'pandas',
      ],
      zip_safe=False)
