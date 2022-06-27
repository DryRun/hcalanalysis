#!/bin/bash
python run_processor.py testprocessor.TestProcessor -q \
   -I filelists/files_r352567_MinimumBias.txt \
   -d r352567_MinimumBias \
   -o test.coffea
   