import sys
from setuptools import setup, find_packages
from asyncnsq import __version__

PY_VER = sys.version_info
install_requires = [
    'python-snappy==0.5.4',
    'aiohttp==3.6.2',
]

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("asyncnsq doesn't support Python version prior 3.3")

classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
]

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='asyncnsq',
    version=__version__.__version__,
    description='NSQ library with native asyncio async/await support',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=classifiers,
    author='Alexander "GinTR1k" Karateev',
    author_email='administrator@gintr1k.space',
    url='https://github.com/list-family/asyncnsq',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    install_requires=install_requires,
    include_package_data=True,
)
