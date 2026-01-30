"""
VeridicalDB Python Driver Setup
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="veridicaldb",
    version="0.1.0",
    author="VeridicalDB Contributors",
    author_email="",
    description="Python driver for VeridicalDB with PostgreSQL wire protocol support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/JayabrataBasu/VeridicalDB",
    packages=find_packages(),
    package_data={
        "veridicaldb": ["py.typed"],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        # No external dependencies required
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.20.0",
            "pytest-cov>=4.0.0",
        ],
    },
    keywords="database veridicaldb postgresql driver client",
    project_urls={
        "Bug Reports": "https://github.com/JayabrataBasu/VeridicalDB/issues",
        "Source": "https://github.com/JayabrataBasu/VeridicalDB",
    },
)
