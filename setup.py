#!/usr/bin/env python3
"""
Setup script for mass_download package
"""
from setuptools import setup, find_packages

setup(
    name="mass_download",
    version="1.0.0",
    description="Mass YouTube channel download system with S3 integration",
    author="Mass Download Development Team",
    author_email="dev@massdownload.system",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "yt-dlp>=2025.8.22",
        "PyYAML>=6.0",
        "psutil>=5.0.0",
        "boto3>=1.26.0",
    ],
    entry_points={
        "console_scripts": [
            "mass-download=mass_download.cli.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "mass_download": ["*.yaml", "*.yml", "*.json"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Multimedia :: Video",
        "Topic :: System :: Archiving",
    ],
    keywords="youtube download mass batch s3 archive",
)