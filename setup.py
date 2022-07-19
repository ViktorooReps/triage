from setuptools import setup, find_packages
from io import open
from os import path

import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# automatically captured required modules for install_requires in requirements.txt and as well as configure dependency links
with open(path.join(HERE, 'requirements.txt'), encoding='utf-8') as f:
    def remove_version(req: str):
        return req.split('==')[0]
    all_reqs = tuple(map(remove_version, f.read().split('\n')))

install_requires = [x.strip() for x in all_reqs if ('git+' not in x) and (not x.startswith('#')) and (not x.startswith('-'))]

dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]

setup(
    name='triage-runner',
    description='Script running tool for optimizing GPU memory utilization.',
    version='0.3.1',
    packages=find_packages(),  # list of all packages
    install_requires=install_requires,
    python_requires='>=3.6',
    entry_points='''
        [console_scripts]
        triage=runner.__main__:main
    ''',
    author="Viktor Scherbakov",
    keyword="Script runner, ML/DL experiments, task runner, GPU job manager",
    long_description=README,
    long_description_content_type="text/markdown",
    license='MIT',
    url='https://github.com/ViktorooReps/triage',
    download_url='https://github.com/ViktorooReps/triage/archive/0.0.1.tar.gz',
    dependency_links=dependency_links,
    author_email='viktoroo.sch@gmail.com',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.11",
    ],
    include_package_data=True,
)
