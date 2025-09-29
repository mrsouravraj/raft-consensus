#!/usr/bin/env python3
"""
Setup configuration for Raft Consensus Algorithm educational implementation.
"""

from setuptools import setup, find_packages
import os

# Read README for long description
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    requirements = []
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    requirements.append(line)
    return requirements

setup(
    name="raft-consensus-educational",
    version="1.0.0",
    author="Advanced Algorithms Series",
    author_email="mr.soura.vraj@gmail.com",
    description="Educational implementation of the Raft Consensus Algorithm in Python",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/mrsouravraj/raft-consensus",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Education",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Education",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "raft-demo=examples.basic_raft_demo:main",
            "raft-apps-demo=examples.real_world_applications_demo:run_demo",
        ],
    },
    keywords=[
        "raft",
        "consensus",
        "distributed-systems",
        "algorithm",
        "education",
        "etcd",
        "consul",
        "cockroachdb",
        "leader-election",
        "log-replication",
    ],
    project_urls={
        "Bug Reports": "https://github.com/mrsouravraj/raft-consensus/issues",
        "Source": "https://github.com/mrsouravraj/raft-consensus",
        "Documentation": "https://github.com/mrsouravraj/raft-consensus#readme",
        "Raft Paper": "https://raft.github.io/raft.pdf",
    },
    include_package_data=True,
    zip_safe=False,
)
