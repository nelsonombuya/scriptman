import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

VERSION = "0.0.0.55"
DESCRIPTION = (
    "ScriptMan is a versatile Python package that simplifies script "
    " management tasks, offering a wide range of utilities for various"
    " scripting needs. Whether you're dealing with data, databases,"
    " command-line interfaces, web automation, or just aiming for better"
    " script organization, ScriptMan has you covered. With features like"
    " cleanup handling, database interaction, file management, logging, and"
    " more, ScriptMan is your go-to tool for efficient and well-organized"
    " scripting in Python."
)

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="scriptman",
    version=VERSION,
    author="Nelson Ombuya",
    description=DESCRIPTION,
    packages=find_packages(),
    long_description=LONG_DESCRIPTION,
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
