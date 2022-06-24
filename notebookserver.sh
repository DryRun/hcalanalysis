#!/bin/bash
if [ -z "$1" ] ; then
    PORT=7778
else
    PORT=$1
fi
jupyter notebook --no-browser --port=$PORT
