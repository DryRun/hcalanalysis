'''
This is a generic launcher for processors, which should be defined in processors/xyz.py. 
This script handles the instantiation of the processor and the handling of input/output files. 
'''

import os
import sys
import time
import re
import yaml
import json
from hcalanalysis.processors import *
from hcalanalysis.schemas import HcalNanoAODSchema
from pprint import pprint
from coffea import processor, util

# Workaround for https://github.com/scikit-hep/uproot4/issues/122
import uproot
uproot.open.defaults["xrootd_handler"] = uproot.source.xrootd.MultithreadedXRootDSource

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run processor ")
    parser.add_argument(dest="processor_name", type=str, help="Name of processor, i.e. bx1processor.BX1Processor")
    parser.add_argument("-q", "--quicktest", action="store_true", help="Run a quick test")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("-i", "--inputfiles", type=str, help="List of input files (comma-separated)")
    input_group.add_argument("-I", "--inputfilestxt", type=str, help="")
    input_group.add_argument("-y", "--inputfilesyaml", type=str, help="")
    input_group.add_argument("-j", "--inputfilesjson", type=str, help="")
    parser.add_argument("-o", "--outputfile", type=str, required=True, help="Output file")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite output file")
    parser.add_argument("-d", "--dataset", type=str, help="Dataset name (used as the key in the output accumulator dictionary)")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("-c", "--condor", action="store_true", help="Flag for running on condor (alters paths/logging/behaviors where necessary)")
    parser.add_argument("--chunksize", type=int, default=250, help="Chunk size")
    parser.add_argument("--maxchunks", type=int, default=None, help="Max chunks")
    args = parser.parse_args()

    processor_class = getattr(globals()[args.processor_name.split(".")[0]], args.processor_name.split(".")[1])

    if args.quicktest:
        args.force = True

    # Input file handling
    fileset = {}
    if args.inputfilesyaml:
        if args.dataset:
            raise RuntimeError("ERROR : Cannot specify --dataset/-d with --inputfilesyaml/-y, because the dataset naming comes from the .yaml file.")

        with open(args.inputfilesyaml) as f:
            doc = yaml.load(f)
            for dataset, filelist in doc.items():
                fileset[dataset] = filelist
    elif args.inputfilesjson:
        if args.dataset:
            raise RuntimeError("ERROR : Cannot specify --dataset/-d with --inputfilesjson/-j, because the dataset naming comes from the .json file.")
        with open(args.inputfilesjson) as f:
            fileset = json.load(f)
    else:
        input_files = []
        if args.inputfiles:
            input_files = args.inputfiles.split(",")
        elif args.inputfilestxt:
            with open(args.inputfilestxt, "r") as f:
                for line in f:
                    input_files.append(line.strip())
        fileset[args.dataset] = input_files

    # Normalize file names
    for k, v in fileset.items():
        new_list = []
        for input_file in v:
            new_input_file = input_file
            if new_input_file.startswith("/eos/cms/store"):
                new_input_file = re.sub(r"^/eos/cms/store", r"root://eoscms.cern.ch//store", new_input_file)
            elif new_input_file.startswith("/store"):
                new_input_file = re.sub(r"^/store", r"root://eoscms.cern.ch//store", new_input_file)
            new_list.append(new_input_file)
        fileset[k] = new_list

    # For quicktest, limit number of input files and events per job
    if args.quicktest:
        for k, v in fileset.items():
            fileset[k] = v[:1]
        maxchunks = 8 # do >=2 chunks to make sure output merging works
    else:
        maxchunks = args.maxchunks

    print("Input fileset:")
    pprint(fileset)

    # Check output file status
    if os.path.isfile(args.outputfile) and not args.force:
        raise RuntimeError("Output file {args.outputfile} already exists. Specify -f/--force to overwrite.")

    # Run processor
    ts_start = time.time()
    output = processor.run_uproot_job(fileset,
                                treename='Events',
                                processor_instance=processor_class(),
                                executor=processor.futures_executor,
                                #executor=processor.iterative_executor,
                                executor_args={'workers': args.workers, 'status': True, 'schema': HcalNanoAODSchema},
                                chunksize=args.chunksize,
                                maxchunks=maxchunks,
                            )
    ts_end = time.time()
    total_time = ts_end - ts_start

    pprint(output)

    # Save output
    util.save(output, args.outputfile)


    # Print some bookkeeping
    # Note: require nevents to be present!
    can_benchmark = True
    for dataset, outputdict in output.items():
        if not "nevents" in outputdict:
            can_benchmark = False
            break

    if can_benchmark:
        total_events = 0
        dataset_nevents = {}
        pprint(output)
        for dataset, outputdict in output.items():
            nevents = outputdict["nevents"]
            if dataset in dataset_nevents:
                dataset_nevents[dataset] += nevents
            else:
                dataset_nevents[dataset] = nevents
            total_events += nevents

        print(f"Processor performance:")
        print(f"\t{total_events} events")
        print(f"\t{total_time:.3f} s")
        print(f"\t{total_events / total_time:.3f} Hz")
    else:
        print("Can't compute processor benchmarks. Create an int `nevents` in the output to enable.")