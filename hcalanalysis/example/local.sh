#!/bin/bash
python ../run_processor.py testprocessor.TestProcessor \
   -y inputfiles.yaml \
   -o hists.coffea \
   -w 4
   