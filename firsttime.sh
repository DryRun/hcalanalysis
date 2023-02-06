#!/bin/bash
#source /cvmfs/sft.cern.ch/lcg/views/LCG_99/x86_64-centos7-clang10-opt/setup.sh
export LCG=/cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-clang11-opt/
source ${LCG}/setup.sh

python -m venv --copies venv
source venv/bin/activate

# Put local packages first in the PYTHONPATH
LOCALPATH=$(pwd)/venv$(python -c 'import sys; print(f"/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages")')

# Edit venv/bin/activate
sed -i '1s/#!.*python$/#!\/usr\/bin\/env python/' venv/bin/*
sed -i '40s/.*/VIRTUAL_ENV="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}" )")" \&\& pwd)"/' venv/bin/activate
sed -i "2a source ${LCG}/setup.sh" venv/bin/activate
sed -i "3a export PYTHONPATH=${LOCALPATH}:\$PYTHONPATH" venv/bin/activate
sed -i "4a export HCALANALYSISDIR=$(pwd)" venv/bin/activate

# Edit PYTHONPATH in the current session, too
export PYTHONPATH=${LOCALPATH}:$PYTHONPATH
export HCALANALYSISDIR=$(pwd)

# Install stuff
python -m pip install setuptools pip wheel --upgrade
python -m pip install coffea
python -m pip install importlib-resources==1.3.0
python -m pip install --editable . --use-pep517

# Make csub available anywhere
cp hcaltools/csub venv/bin
chmod a+x venv/bin/csub
