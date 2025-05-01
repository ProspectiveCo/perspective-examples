from setuptools import setup, find_packages
import sys

# Ensure the script is run with Python 3
if sys.version_info < (3, 0):
    sys.exit("Python 3 or above is required.")

# Read the requirements from requirements.txt
with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setup(
    name='prospective_demo_dataset',
    version='0.1',
    packages=find_packages(include=['prospective_demo_dataset', 'prospective_demo_dataset.*', 'tests']),
    install_requires=install_requires,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    author='xdatanomad',
    author_email='parham@prospective.co',
    description='A utility for simulating streaming data to Prospective and Perspective.',
    url='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    python_requires='>=3.0',
)
