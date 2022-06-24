import logging
import numpy as np
import awkward as ak
import json
import copy
from collections import defaultdict
from coffea import processor
from coffea import hist as chist
import hist
logger = logging.getLogger(__name__)
import time
from pprint import pprint

def main():
	runs = ["352493", "352499", "352505", "352509"]
	fileset = {}
	for run in runs:
	    fileset[run] = []
	    with open(f"hcalnano_r{run}.txt", "r") as f:
	        for line in f:
	            fileset[run].append(line.strip().replace("/eos/cms/store", "root://eoscms.cern.ch//store"))
	#events = NanoEventsFactory.from_root(fn, metadata={"dataset": ds}, entry_stop=100000).events()

	ts_start = time.time()
	#pprint(fileset)

	dev = True
	if dev:
	    for k, v in fileset.items():
	        fileset[k] = v[:2]
	output = processor.run_uproot_job(fileset,
	                            treename='Events',
	                            processor_instance=PhaseScanProcessor(),
	                            #executor=processor.futures_executor,
	                            executor=processor.iterative_executor,
	                            executor_args={'workers': 1, 'status': True},
	                            chunksize=500,
	                            maxchunks=1,
	                        )
	ts_end = time.time()
	total_events = 0
	dataset_nevents = {}
	for k, v in output['nevents'].items():
	    if k in dataset_nevents:
	        dataset_nevents[k] += v
	    else:
	        dataset_nevents[k] = v
	    total_events += v



class PhaseScanProcessor(processor.ProcessorABC):
    def __init__(self):
        self._axis_ls = hist.axis.IntCategory([], name="ls", growth=True)
        self._axis_subdet = hist.axis.IntCategory([], name="subdet", growth=True)
        self._axis_ieta = hist.axis.Regular(83, -41.5, 41.5, name="ieta", label=r"ieta")
        self._axis_ieta_hb = hist.axis.Regular(35, -17.5, 17.5, name="ieta", label=r"ieta")
        self._axis_ieta_he = hist.axis.Regular(59, -29.5, 29.5, name="ieta", label=r"ieta")
        self._axis_iphi = hist.axis.Regular(72, 0.5, 72.5, name="iphi", label=r"iphi")
        self._axis_depth4 = hist.axis.Regular(4, 0.5, 3.5, name="depth", label=r"depth")
        self._axis_depth7 = hist.axis.Regular(7, 0.5, 7.5, name="depth", label=r"depth")
        self._axis_tdc_he = hist.axis.Regular(50, -0.5, 49.5, name="tdc", label=r"TDC (HE)")
        self._axis_tdc_hb = hist.axis.Regular(4, -0.5, 3.5, name="tdc", label=r"TDC (HB)")
        self._axis_tstime = hist.axis.Regular(25, 0., 25., name="tstime", label=r"Q-weighted TS [ns]")

        self.make_output = lambda: {
            '''
            'hist_tdctime': hist.Hist(
                self._axis_ls,
                self._axis_subdet,
                self._axis_ieta, 
                self._axis_iphi, 
                self._axis_depth,
                self._axis_tdctime
            ), 
            '''
            'hist_tstime_hb': hist.Hist(
                self._axis_ls,
                self._axis_ieta_hb, 
                self._axis_iphi, 
                self._axis_depth4,
                self._axis_tstime
            ), 
            'nevents': processor.defaultdict_accumulator(int),
        }

    def process(self, events):
        print(events.__dict__)
        output = self.make_output()
        output['nevents'][events.metadata["dataset"]] += len(events)
        
        # Repackage time slice arrays
        setattr(events.HBDigis, "adc", ak.concatenate([getattr(events.HBDigis, f"adc{iTS}") for iTS in range(8)], axis=-1))
        setattr(events.HBDigis, "fc", ak.concatenate([getattr(events.HBDigis, f"fc{iTS}") for iTS in range(8)], axis=-1))
        setattr(events.HBDigis, "pedestalfc", ak.concatenate([getattr(events.HBDigis, f"fc{iTS}") for iTS in range(8)], axis=-1))
        setattr(events.HBDigis, "realfc", events.HBDigis.fc - events.HBDigis.pedestalfc)
        
        hb_digi_mask = events.HBDigis.valid
        hb_digis = events.HBDigis[hb_digi_mask]
        
        qsum = ak.zeroes_like(hb_digis.tdc0)
        qwsum = ak.zeroes_like(hb_digis.tdc0)
        for iTS in range(8):
            qwsum += iTS * getattr(hbdigis, f"fc{iTS}")
            qsum += getattr(hbdigis, f"fc{iTS}")
        setattr(events.HBDigis, "tstime", qwsum / qsum)
        
        output["hist_tstime_hb"].fill(
            ls = events.luminosityBlock, 
            ieta = ak.flatten(hb_digis.ieta), 
            iphi = ak.flatten(hb_digis.iphi), 
            depth = ak.flatten(hb_digis.depth), 
            tstime = ak.flatten(hbdigis.tstime)
        )
        
        return processor.accumulate({events.metadata["dataset"]: output})

    def postprocess(self, accumulator):
        return accumulator    
    
if __name__ == "__main__":
	main()