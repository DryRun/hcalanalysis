import os
import sys
import time
import re
import yaml
import json
from pprint import pprint

import logging
import numpy as np
import awkward as ak
import copy
from collections import defaultdict
from coffea import processor, util
from coffea import hist as chist
from coffea import nanoevents
import hist

# Workaround for https://github.com/scikit-hep/uproot4/issues/122
import uproot
uproot.open.defaults["xrootd_handler"] = uproot.source.xrootd.MultithreadedXRootDSource

logger = logging.getLogger(__name__)

# Uncomment this for verbose array printing (warning: this can be huge!)
#np.set_printoptions(threshold=sys.maxsize)

class SplashProcessor(processor.ProcessorABC):
    def __init__(self, ):
        # List of (run, event_number) of splash events, once known
        # You can you the find_events histogram to find the splashes
        self._splash_event_list = [("r365373_Splashes_FEVT", 216), 
                         ("r365373_Splashes_FEVT", 217), 
                         ("r365373_Splashes_FEVT", 1258), 
                         ("r365373_Splashes_FEVT", 1520), 
                         ("r365373_Splashes_FEVT", 1521), ] 

        self._axis_subdet = hist.axis.StrCategory(["HB", "HE", "HF"], name="subdet", growth=True)
        self._axis_ieta   = hist.axis.Regular(83, -41.5, 41.5, name="ieta", label=r"ieta")
        self._axis_iphi   = hist.axis.Regular(72, 0.5, 72.5, name="iphi", label=r"iphi")
        self._axis_depth7 = hist.axis.Regular(7, 0.5, 7.5, name="depth", label=r"depth")
        self._axis_adc256 = hist.axis.Regular(256, -0.5, 255.5, name="adc", label=r"ADC")
        self._axis_sumq   = hist.axis.Variable(
                                10**np.linspace(0, 7, num=1000, endpoint=True), 
                                name="sumq", 
                                label=r"Channel $\Sigma_{TS} q$ [fC]"
                            )


        self.make_output = lambda: {
            'nevents': 0,
            'event_sumq': hist.Hist(
                hist.axis.IntCategory([], name='event_number', label=r"Event Number", growth=True), 
            ), 
            'event_sumq_dict': processor.accumulator.dict_accumulator(), 
            "splash_depthmap": hist.Hist(
                hist.axis.IntCategory([], name='event_number', label=r"Event Number", growth=True), 
                self._axis_ieta, 
                self._axis_iphi, 
                self._axis_depth7
            ),
            "splash_tdctime": hist.Hist(
                hist.axis.IntCategory([], name='event_number', label=r"Event Number", growth=True), 
                self._axis_ieta, 
                self._axis_iphi, 
                self._axis_depth7
            ),

        }

    def process(self, events):
        #print(events.__dict__)
        output = self.make_output()
        output['nevents'] = len(events)

        digis = {}
        nTS = {"HB": 8, "HE": 8, "HF": 3}
        event_sumq = np.zeros_like(ak.to_numpy(events["event"]))
        for subdet in ["HB", "HE", "HF"]:
            digis[subdet] = events[f"Digi{subdet}"]
            digis[subdet] = digis[subdet][digis[subdet].valid]
            digis[subdet]["sumq"] = sum([digis[subdet][f"fc{iTS}"] for iTS in range(nTS[subdet])])
            event_sumq = event_sumq + ak.to_numpy(ak.sum(digis[subdet]["sumq"], axis=-1))

        output["event_sumq"].fill(
            event_number = events["event"], 
            weight = event_sumq
        )
        output["event_sumq_dict"] = dict(zip(events["event"], event_sumq))

        return processor.accumulate([{events.metadata["dataset"]: output}])

    def postprocess(self, accumulator):
        return accumulator    
    
'''
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run BX1 processor")
    parser.add_argument("-q", "--quicktest", action="store_true", help="Run a quick test")
    parser.add_argument()
    args = parser.parse_args()

    # Make fileset
    fileset = {}
    datasets = ["MinimumBias", "HcalNZS"]
    runs = ["352567"]
    for run in runs:
        for dataset in ["MinimumBias", "HcalNZS"]:
            tag = f"{dataset}_{run}"
            fileset[tag] = []
            with open("/afs/cern.ch/user/d/dryu/HCAL/HcalAnalysis/hcalanalysis/files_r{run}_{dataset}.txt", "r") as f:
                for line in f:
                    fileset[tag].append(line.strip().replace("/eos/cms/store", "root://eoscms.cern.ch//store"))
    pprint(fileset)

    ts_start = time.time()
    #pprint(fileset)

    if args.quicktest:
        # Limit number of input files and events per job
        for k, v in fileset.items():
            fileset[k] = v[:1]
        maxchunks = 2
    else:
        maxchunks = -1

    output = processor.run_uproot_job(fileset,
                                treename='Events',
                                processor_instance=BX1Processor(),
                                #executor=processor.futures_executor,
                                executor=processor.iterative_executor,
                                executor_args={'workers': 1, 'status': True, 'schema': processor.NanoAODSchema},
                                chunksize=500,
                                maxchunks=maxchunks,
                            )
    ts_end = time.time()
    total_events = 0
    dataset_nevents = {}
    print(output)
    for dataset, outputdict in output.items():
        nevents = outputdict["nevents"]
        if dataset in dataset_nevents:
            dataset_nevents[dataset] += nevents
        else:
            dataset_nevents[k] = nevents
        total_events += nevents
'''

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Find splash events")
    parser.add_argument("-q", "--quicktest", action="store_true", help="Run a quick test")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("-i", "--inputfiles", type=str, help="List of input files (comma-separated)")
    input_group.add_argument("-I", "--inputfilestxt", type=str, help="")
    input_group.add_argument("-y", "--inputfilesyaml", type=str, help="")
    input_group.add_argument("-j", "--inputfilesjson", type=str, help="")
    parser.add_argument("-o", "--outputfile", type=str, required=True, help="Output file")
    #parser.add_argument("-f", "--force", action="store_true", help="Overwrite output file")
    parser.add_argument("-d", "--dataset", type=str, help="Dataset name (used as the key in the output accumulator dictionary)")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("-c", "--condor", action="store_true", help="Flag for running on condor (alters paths/logging/behaviors where necessary)")
    parser.add_argument("--chunksize", type=int, default=250, help="Chunk size")
    parser.add_argument("--maxchunks", type=int, default=None, help="Max chunks")
    args = parser.parse_args()

    processor_class = SplashProcessor

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
    #if os.path.isfile(args.outputfile) and not args.force:
    #    raise RuntimeError("Output file {args.outputfile} already exists. Specify -f/--force to overwrite.")

    # Run processor
    ts_start = time.time()
    output = processor.run_uproot_job(fileset,
                                treename='Events',
                                processor_instance=processor_class(),
                                executor=processor.futures_executor,
                                #executor=processor.iterative_executor,
                                executor_args={'workers': args.workers, 'status': True, 'schema': processor.HcalNanoAODSchema},
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

    for run_key in output.keys():
        print(run_key)
        sumq_list = list(output[run_key]["event_sumq_dict"].items())
        sumq_list = sorted(sumq_list, key=lambda x: x[1], reverse=True)
        print(len(sumq_list))
        pprint(sumq_list[:20])