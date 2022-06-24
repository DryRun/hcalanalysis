#!/bin/bash
python run_processor.py bx1processor.BX1Processor -q \
   -I filelists/files_r352567_MinimumBias.txt \
   -d MinimumBias \
   -o test.coffea
   