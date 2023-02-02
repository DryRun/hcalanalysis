#!/bin/bash
if [ -z "$1" ]; then
	PORT=7778;
else
	PORT="$1";
fi
ssh -L localhost:${PORT}:localhost:${PORT} dryu@lxplus.cern.ch "cd HCAL/hcalanalysis; echo 'Setting up env...'; source env.sh; echo 'Launching notebook...'; source notebookserver.sh ${PORT};"
