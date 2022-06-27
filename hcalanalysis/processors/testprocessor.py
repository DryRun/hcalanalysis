import logging
import numpy as np
import awkward as ak
import json
import copy
from collections import defaultdict
from coffea import processor
from coffea import hist as chist
from coffea import nanoevents
import hist
logger = logging.getLogger(__name__)
import time
from pprint import pprint
import sys

# Uncomment this for verbose array printing (warning: this can be huge!)
#np.set_printoptions(threshold=sys.maxsize)

class TestProcessor(processor.ProcessorABC):
    def __init__(self):
        self._axis_subdet = hist.axis.StrCategory(["HB", "HE"], name="subdet", growth=True)
        self._axis_ieta = hist.axis.Regular(83, -41.5, 41.5, name="ieta", label=r"ieta")
        self._axis_iphi = hist.axis.Regular(72, 0.5, 72.5, name="iphi", label=r"iphi")
        self._axis_depth7 = hist.axis.Regular(7, 0.5, 7.5, name="depth", label=r"depth")
        
        self.make_output = lambda: {
            'nevents': 0,
            "eventq": hist.Hist(
                hist.axis.IntCategory([0, 1], name="is_bx1", growth=False),
                hist.axis.Variable(
                            10**np.linspace(2, 8, num=1000, endpoint=True), 
                            name="eventq", 
                            label=r"Total event $q$ [fC]"
                ),
            ),
            "sumq": hist.Hist(
                self._axis_subdet, 
                hist.axis.IntCategory([0, 1], name="is_bx1", growth=False),
                hist.axis.Variable(
                            10**np.linspace(0, 7, num=1000, endpoint=True), 
                            name="sumq", 
                            label=r"Channel $\Sigma_{TS} q$ [fC]"
                ),
            ),
            'sumq_depthmap': hist.Hist(
                self._axis_ieta,
                self._axis_iphi,
                self._axis_depth7,
            ), 
            "interesting_events": processor.list_accumulator([]),

        }

    def process(self, events):
        #print(events.__dict__)
        output = self.make_output()
        output['nevents'] = len(events)
        #print(f"nevents = {len(events)}")
        
        is_bx1 = (events.bunchCrossing == 1)

        # Container for storing total event charge (1D array; i.e., one number per event)
        eventq = ak.zeros_like(events.event, dtype=float)
        digis = {}
        validdigis = {}
        for subdet in ["HB", "HE"]:
            #print(f"Working on {subdet}")
            digis[subdet] = events[f"{subdet}Digis"]

            # In hcalnano, time slices are saved as individual branches (due to NanoAOD's limit on dimensionality of output)
            # We can repack them into a single object as follows (however, it's a bit time consuming)
            digis[subdet]["adc"] = np.stack([ak.to_numpy(digis[subdet][f"adc{iTS}"]) for iTS in range(8)], axis=-1)
            digis[subdet]["fc"] = np.stack([ak.to_numpy(digis[subdet][f"fc{iTS}"]) for iTS in range(8)], axis=-1)
            digis[subdet]["pedestalfc"] = np.stack([ak.to_numpy(digis[subdet][f"pedestalfc{iTS}"]) for iTS in range(8)], axis=-1)

            # Compute some derived quantities
            digis[subdet]["realfc"] = digis[subdet].fc - digis[subdet].pedestalfc
            digis[subdet]["sumq"] = ak.sum(digis[subdet]["realfc"], axis=-1)

            # Apply a mask on "valid digis". 
            #    "valid" means that the digi was filled during the hcalnano step;
            #    invalid digis are filled with default values, and should never be filled into histograms!
            validdigis[subdet] = digis[subdet][digis[subdet].valid]

            eventq = eventq + ak.sum(validdigis[subdet]["sumq"], axis=-1)

            # Fill histograms
            
            # Techicality: variables for filling must be either 1D arrays (all same length) or scalar
            #    Hence event quantities (e.g. event number, BX number) have to be 
            #    broadcast to the same shape as the digi arrays, and multidimensional arrays have 
            #    to be flattened (use ak.flatten() )
            validdigis_shape = ak.ones_like(validdigis[subdet].ieta)
            is_bx1_validdigishape = is_bx1 * validdigis_shape
            
            # Channel total charge (sumq)
            #    Note that arrays must be 1D, so we call ak.flatten on the 2D digi arrays (recall, digis are stored as <evt num : detid>)
            output["sumq"].fill(
                subdet = subdet, 
                sumq = ak.flatten(validdigis[subdet]["sumq"]), 
                is_bx1 = ak.flatten(is_bx1_validdigishape), 
            )

            # sumq depth map
            output["sumq_depthmap"].fill(
                ieta   = ak.flatten(validdigis[subdet].ieta), 
                iphi   = ak.flatten(validdigis[subdet].iphi), 
                depth  = ak.flatten(validdigis[subdet].depth), 
                weight = ak.flatten(validdigis[subdet]["sumq"])
            )

        # Fill event total charge
        output["eventq"].fill(
            is_bx1 = is_bx1, 
            eventq = eventq, 
        )


        # Record some interesting event numbers 
        # Define a highq hit as (sumq > 200,000 fC) (value roughly chosen based on https://cmsweb.cern.ch/dqm/online/start?runnr=324980;dataset=/Global/Online/ALL;sampletype=online_data;filter=all;referencepos=overlay;referenceshow=customise;referencenorm=True;referenceobj1=refobj;referenceobj2=none;referenceobj3=none;referenceobj4=none;search=;striptype=object;stripruns=;stripaxis=run;stripomit=none;workspace=HCAL;size=M;root=Hcal/DigiTask/SumQ/SubdetPM;focus=Hcal/DigiTask/SumQ/SubdetPM/HEM;zoom=yes;)
        # Interesting event == (>10 highq hits)
        #highq_nhits = ak.zeros_like(events.event, dtype=float)
        #for subdet in ["HB", "HE"]:
        #    highq_nhits = highq_nhits + ak.sum(validdigis[subdet]["sumq"] > 2.e5, axis=-1)
        #interesting_events = events.event[(highq_nhits >= 10)]

        # OK, counting high energy hits turns out to be a hard way to define interesting events :) Instead, do eventq>20,000 fC
        interesting_events = events.event[(eventq>2.e5)]
        print(f"Interesting events: {len(interesting_events)} / {len(events)}")
        output["interesting_events"].extend(interesting_events)
    


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
