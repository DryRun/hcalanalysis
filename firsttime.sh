#!/bin/bash
#source /cvmfs/sft.cern.ch/lcg/views/LCG_99/x86_64-centos7-clang10-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-clang11-opt/setup.sh

python -m venv --copies venv
echo "export PYTHONPATH=${PWD}/venv/lib/python3.8/site-packages:$PYTHONPATH" >> venv/bin/activate
#/user_data/dryu/BFrag/boffea/venv/lib/python3.8/site-packages:/user_data/dryu/BFrag/boffea:$PYTHONPATH
#cp venv_95/bin/csub venv/bin/

source venv/bin/activate
#python -m pip install setuptools pip --upgrade
#python -m pip install coffea
#python -m pip install xxhash
pip install --editable . 
