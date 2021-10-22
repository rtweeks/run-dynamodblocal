# Copyright 2021 Richard T. Weeks
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

ver_info = {'__file__': 'lib/run_dynamodblocal/version.py'}
with open(ver_info['__file__']) as vf:
    exec(vf.read(), ver_info)

setuptools.setup(
    name="run-dynamodblocal",
    version=ver_info['__version__'],
    author="Richard T. Weeks",
    author_email="rtweeks21@gmail.com",
    description="Run an in-memory DynamoDBLocal for testing",
    license='Apache License 2.0',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rtweeks/run-dynamodblocal",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages('lib'),
    package_dir={'': 'lib'},
    python_requires='>=3.6',
    install_requires=[
        "psutil~=5.8; sys.platform == 'win32'",
    ],
    extras_require={
        'boto3': [
            'boto3_mocking<2'
        ],
    },
)
