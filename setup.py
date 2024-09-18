import os

from setuptools import find_packages, setup

VERSION = "0.0.5"
DESCRIPTION = "A Task Runner Framework"


def long_description(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def requirements(fname):
    return (
        open(
            os.path.join(
                os.path.dirname(__file__),
                fname,
            )
        )
        .read()
        .splitlines()
    )


setup(
    name="task-racer",
    version=VERSION,
    author="Bintang Pradana Erlangga Putra",
    author_email="<work.bpradana@gmail.com>",
    description=DESCRIPTION,
    long_description=long_description("README.md"),
    long_description_content_type="text/markdown",
    url="https://github.com/bpradana/racer",
    packages=find_packages(),
    install_requires=requirements("requirements.txt"),
    keywords=["python", "race condition", "task", "runner"],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
    ],
    license="MIT",
)
