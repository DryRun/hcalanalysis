import sys
import os.path
from setuptools import (
    setup,
    find_packages,
)

about = {}
with open(os.path.join("hcalanalysis", "version.py")) as f:
    exec(f.read(), about)


needs_pytest = {"pytest", "test", "ptr"}.intersection(sys.argv)
pytest_runner = ["pytest-runner"] if needs_pytest else []

setup(
    name='hcalanalysis',
    version = "0.0.0",
    packages=[],
    description="A package for analyzing CMS HCAL digi/rechit data",
    long_description=open("README.md", "rb").read().decode("utf8", "ignore"),
    long_description_content_type="text/markdown",
    maintainer="David Yu",
    maintainer_email="david.renhwa.yu@gmail.com",
    url="https://github.com/HCALPFG/HcalAnalysis",
    download_url="https://github.com/HCALPFG/HcalNanoAnalysis/releases",
    license="BSD 3-clause",
    include_package_data=True,
    install_requires=[
          "coffea>=0.7.2",
          #"correctionlib>=2.0.0rc6",
          #"rhalphalib",
          "pandas",
    ],
    setup_requires=["flake8"] + pytest_runner,
    classifiers=[
          # "Development Status :: 4 - Beta",
          "Intended Audience :: Science/Research",
          "License :: OSI Approved :: BSD License",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Topic :: Scientific/Engineering :: Physics",
      ],
)
