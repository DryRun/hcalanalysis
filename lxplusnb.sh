#!/bin/bash
if [ -z "$2" ]; then
	PORT=7778;
else
	PORT="$2";
fi
ssh -N -L localhost:${PORT}:localhost:${PORT} dryu@lxplus${1}.cern.ch
