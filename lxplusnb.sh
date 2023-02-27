#!/bin/bash
if [ -z "$1" ]; then
	PORT=7778;
else
	PORT="$1";
fi
export PORT=7778
export USERNAME=$USER # Edit me!
export LXPLUSFOLDER=HCAL/hcalanalysis # Edit me!
ssh -L localhost:${PORT}:localhost:${PORT} ${USERNAME}@lxplus.cern.ch "cd ${LXPLUSFOLDER}; echo 'Setting up env...'; source env.sh; echo 'Launching notebook...'; source notebookserver.sh ${PORT};"
