import os
import sys
import time
from hcalanalysis.processors import *
from pprint import pprint
from coffea import processor, util

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run processor ")
    parser.add_argument(dest="processor_name", type=str, help="Name of processor, i.e. bx1processor.BX1Processor")
    parser.add_argument("-q", "--quicktest", action="store_true", help="Run a quick test")
    parser.add_argument("-i", "--inputfiles", type=str, help="List of input files (comma-separated)")
    parser.add_argument("-I", "--inputfilestxt", type=str, help="")
    parser.add_argument("-o", "--outputfile", type=str, required=True, help="Output file")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite output file")
    parser.add_argument("-d", "--dataset", type=str, help="Dataset name (used as the key in the output accumulator dictionary)")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("-c", "--condor", action="store_true", help="Flag for running on condor (alters paths/logging/behaviors where necessary)")
    parser.add_argument("--chunksize", type=int, default=500, help="Chunk size")
    parser.add_argument("--maxchunks", type=int, default=None, help="Chunk size")
    args = parser.parse_args()

    processor_class = getattr(globals()[args.processor_name.split(".")[0]], args.processor_name.split(".")[1])

    if args.quicktest:
        args.force = True
        args.dataset = "testdataset"
        args.outputfile = f"test_{args.processor_name.replace('.', '_')}.coffea"

    # Input file handling
    input_files = []
    if args.inputfiles and args.inputfilestxt:
        raise ValueError("Inconsistent arguments -i/--inputfiles and -I/--inputfilestxt")       
    if args.inputfiles:
        input_files = args.inputfiles.split(",")
    elif args.inputfilestxt:
        with open(args.inputfilestxt, "r") as f:
            for line in f:
                input_files.append(line.strip())
    else:
        raise ValueError("No input files specified (-i or -I)")
    for input_file in input_files:
        if input_file.startswith("/eos/cms/store"):
            input_file = re.sub(r"^/eos/cms/store", r"root://eoscms.cern.ch//store", input_file)
        elif input_file.startswith("/store"):
            input_file = re.sub(r"^/store", r"root://eoscms.cern.ch//store", input_file)

    fileset = {args.dataset: input_files}
    if args.quicktest:
        # Limit number of input files and events per job
        for k, v in fileset.items():
            fileset[k] = v[:1]
        maxchunks = 2 # do >=2 chunks to make sure output merging works
    else:
        maxchunks = None

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
                                #executor=processor.futures_executor,
                                executor=processor.iterative_executor,
                                executor_args={'workers': args.workers, 'status': not args.condor, 'schema': processor.NanoAODSchema},
                                chunksize=args.chunksize,
                                maxchunks=args.maxchunks,
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