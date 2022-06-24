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

class BX1Processor(processor.ProcessorABC):
    def __init__(self):
        self._axis_ls = hist.axis.IntCategory([], name="ls", growth=True)
        self._axis_bx = hist.axis.IntCategory([], name="bx", growth=True)
        self._axis_evt = hist.axis.IntCategory([], name="evt", growth=True)
        self._axis_subdet = hist.axis.StrCategory(["HB", "HE"], name="subdet", growth=True)
        self._axis_ieta = hist.axis.Regular(83, -41.5, 41.5, name="ieta", label=r"ieta")
        self._axis_ieta_hb = hist.axis.Regular(35, -17.5, 17.5, name="ieta", label=r"ieta")
        self._axis_ieta_he = hist.axis.Regular(59, -29.5, 29.5, name="ieta", label=r"ieta")
        self._axis_ieta_hbhe = hist.axis.Regular(59, -29.5, 29.5, name="ieta", label=r"ieta")
        self._axis_iphi = hist.axis.Regular(72, 0.5, 72.5, name="iphi", label=r"iphi")
        self._axis_depth4 = hist.axis.Regular(4, 0.5, 3.5, name="depth", label=r"depth")
        self._axis_depth7 = hist.axis.Regular(7, 0.5, 7.5, name="depth", label=r"depth")
        self._axis_adc = hist.axis.Regular(128, -0.5, 255.5, name="adc", label="ADC")

        eventq_bins = 10**np.linspace(3, 6, num=100, endpoint=True)
        nhits_bins = 10**np.linspace(1, 4, num=100, endpoint=True)
        
        self.make_output = lambda: {
            'nevents': 0,
#            'adc_evt': hist.Hist(
#                hist.axis.Boolean(name="is_bx1"),
#                self._axis_evt,
#                self._axis_subdet,
#                self._axis_ieta_hbhe, 
#                self._axis_iphi, 
#                self._axis_depth7,
#                hist.axis.Regular(8, -0.5, 7.5, name="ts", label=r"TS"),
#                #self._axis_adc
#            ), 
#            'sumq_depthmap_evt': hist.Hist(
#                hist.axis.Boolean(name="is_bx1"),
#                self._axis_evt,
#                self._axis_ieta_hbhe, 
#                self._axis_iphi, 
#                self._axis_depth7,
#                hist.axis.Regular(8, -0.5, 7.5, name="ts", label=r"TS"),
#                #self._axis_adc
#            ), 
            'eventq': hist.Hist(
                self._axis_bx,
                self._axis_subdet,
                hist.axis.Variable(eventq_bins, name="eventq", label="Event total charge [fC]"), 
                hist.axis.Variable(nhits_bins, name="nhits", label="Number of hits >60fC")
            ), 
            'nevents': processor.defaultdict_accumulator(int),
        }

    def process(self, events):
        #print(events.__dict__)
        output = self.make_output()
        output['nevents'] = len(events)
        print(f"nevents = {len(events)}")
        
        # Repackage time slice arrays
        #print(events.HBDigis)
        #print(type(events.HBDigis))
        #print(events["HBDigis", "adc3"])
        np.set_printoptions(threshold=sys.maxsize)

        for subdet in ["HB", "HE"]:
            digis = events[f"{subdet}Digis"]
            digis["adc"] = np.stack([ak.to_numpy(digis[f"adc{iTS}"]) for iTS in range(8)], axis=-1)
            digis["fc"] = np.stack([ak.to_numpy(digis[f"fc{iTS}"]) for iTS in range(8)], axis=-1)
            digis["pedestalfc"] = np.stack([ak.to_numpy(digis[f"pedestalfc{iTS}"]) for iTS in range(8)], axis=-1)
            digis["realfc"] = digis.fc - digis.pedestalfc
            digis["sumq"] = ak.sum(digis["realfc"], axis=-1)

            validdigis = digis[digis.valid]
            eventq = ak.sum(validdigis["sumq"][validdigis["sumq"] > 60], axis=-1)
            nhits = ak.num(validdigis["sumq"] > 60., axis=-1)
            
            # Broadcast event# and bx# to the same shape as the digi arrays
            digi_event = events.event * ak.ones_like(validdigis.ieta)
            digi_bx = events.bunchCrossing * ak.ones_like(validdigis.ieta)
            digi_is_bx1 = (digi_bx==1)
            
            # Find a few problem events
            #highq_eventmask = ak.to_list((nhits > 4e2) & (eventq > 5.e4))
            #print("highq_eventmask:")
            #print(highq_eventmask)
            #print(type(highq_eventmask))
            ##print("highq_eventmask count:")
            ##print(ak.count_nonzero(highq_eventmask))
            ##print("Bad nhits:")
            ##print(nhits[highq_eventmask])
            ##print("Bad eventq:")
            ##print(eventq[highq_eventmask])
            ##maxevts = 0
            ##truecount = 0
            #highq_events = []
            #for i in range(len(highq_eventmask)):
            #    if highq_eventmask[i]:
            #        highq_events.append(i)
            #highq_evnets = np.random.choice(highq_events, size=3, replace=False)

            #        truecount += 1
            #        #print(f"High charge event {events.event[i]}")
            #        #print("HLT:")
            #        for trigger in events["HLT"].fields:
            #            if events["HLT"][trigger][i]:
            #                #print(trigger)
            #                pass
            #    if truecount >= maxevts+1:
            #        highq_eventmask[i] = False

            #print(ak.count(highq_eventmask))
            # ADC per event, for looking at pulse shapes
            
            #for ts in range(8):  
            #    output["adc_evt"].fill(
            #        evt = ak.flatten(digi_event[highq_eventmask]),
            #        is_bx1 = ak.flatten(digi_is_bx1[highq_eventmask]),
            #        ts = ts,
            #        subdet = subdet,
            #        ieta = ak.flatten(validdigis.ieta[highq_eventmask]), 
            #        iphi = ak.flatten(validdigis.iphi[highq_eventmask]), 
            #        depth = ak.flatten(validdigis.depth[highq_eventmask]), 
            #        weight = ak.flatten(validdigis[f"adc{ts}"][highq_eventmask]), 
            #    )
            
            # Event total charge vs nHits
            output["eventq"].fill(
                bx = events.bunchCrossing, 
                subdet = subdet, 
                eventq = eventq, 
                nhits = nhits
            )
            
        
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
