import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

VERSION = "0.0.0.11"
DESCRIPTION = "Script Manager assists with managing python scripts."

setup(
    name="scriptman",
    version=VERSION,
    author="Nelson Ombuya",
    description=DESCRIPTION,
    packages=find_packages(),
    author_email="nelson.ombuya@zohomail.com",
    long_description_content_type="text/markdown",
    keywords=["python", "scripts", "etl", "selenium"],
    install_requires=[
        "tqdm",
        "pyodbc",
        "pandas",
        "selenium",
        "requests",
        "webdriver_manager",
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
)
