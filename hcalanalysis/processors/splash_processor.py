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

class SplashProcessor(processor.ProcessorABC):
    def __init__(self, ):
        # List of (run, event_number) of splash events, once known
        # You can you the find_events histogram to find the splashes
        self._splash_event_list = [("r365373_Splashes_FEVT", 216), 
                         ("r365373_Splashes_FEVT", 217), 
                         ("r365373_Splashes_FEVT", 1258), 
                         ("r365373_Splashes_FEVT", 1520), 
                         ("r365373_Splashes_FEVT", 1521), 
                         ("r365537_Splashes_FEVT", 6675), ("r365537_Splashes_FEVT", 6802), ("r365537_Splashes_FEVT", 6461), ("r365537_Splashes_FEVT", 6742), ("r365537_Splashes_FEVT", 7244), ("r365537_Splashes_FEVT", 7782), ("r365537_Splashes_FEVT", 7307), ("r365537_Splashes_FEVT", 6601), ("r365537_Splashes_FEVT", 7947), ("r365537_Splashes_FEVT", 8004), ("r365537_Splashes_FEVT", 8118), ("r365537_Splashes_FEVT", 8065), ("r365537_Splashes_FEVT", 6536), ("r365537_Splashes_FEVT", 7617), ("r365537_Splashes_FEVT", 6397), ("r365537_Splashes_FEVT", 7834), ("r365537_Splashes_FEVT", 8181), ("r365537_Splashes_FEVT", 7133), ("r365537_Splashes_FEVT", 7086), ("r365537_Splashes_FEVT", 10315), ("r365537_Splashes_FEVT", 7570), ("r365537_Splashes_FEVT", 7885), ("r365537_Splashes_FEVT", 9173), ("r365537_Splashes_FEVT", 10430), ("r365537_Splashes_FEVT", 9116), ("r365537_Splashes_FEVT", 10044), ("r365537_Splashes_FEVT", 10386), ("r365537_Splashes_FEVT", 8606), ("r365537_Splashes_FEVT", 10103), ("r365537_Splashes_FEVT", 9280), ("r365537_Splashes_FEVT", 8494), ("r365537_Splashes_FEVT", 10160), ("r365537_Splashes_FEVT", 8723), ("r365537_Splashes_FEVT", 9332), ("r365537_Splashes_FEVT", 8834), ("r365537_Splashes_FEVT", 8670), ("r365537_Splashes_FEVT", 8788), ("r365537_Splashes_FEVT", 8386), ("r365537_Splashes_FEVT", 9772), ("r365537_Splashes_FEVT", 9393), ("r365537_Splashes_FEVT", 10272), ("r365537_Splashes_FEVT", 8549), ("r365537_Splashes_FEVT", 7134), ("r365537_Splashes_FEVT", 6462), ("r365537_Splashes_FEVT", 8182), ("r365537_Splashes_FEVT", 7571), ("r365537_Splashes_FEVT", 8119), ("r365537_Splashes_FEVT", 9333), ("r365537_Splashes_FEVT", 8005), ("r365537_Splashes_FEVT", 6463), ("r365537_Splashes_FEVT", 7135), ("r365537_Splashes_FEVT", 8551), ("r365537_Splashes_FEVT", 9334), ("r365537_Splashes_FEVT", 9174), ("r365537_Splashes_FEVT", 7087), ("r365537_Splashes_FEVT", 6537), ("r365537_Splashes_FEVT", 8550), ("r365537_Splashes_FEVT", 8835), ("r365537_Splashes_FEVT", 8607), ("r365537_Splashes_FEVT", 8183), ("r365537_Splashes_FEVT", 10161), ("r365537_Splashes_FEVT", 8789), ("r365537_Splashes_FEVT", 6398), ("r365537_Splashes_FEVT", 9773), ("r365537_Splashes_FEVT", 7948), ("r365537_Splashes_FEVT", 9394), ("r365537_Splashes_FEVT", 8671), ("r365537_Splashes_FEVT", 9175), ("r365537_Splashes_FEVT", 8120), ("r365537_Splashes_FEVT", 6399), ("r365537_Splashes_FEVT", 8552), ("r365537_Splashes_FEVT", 10162), ("r365537_Splashes_FEVT", 8006), ("r365537_Splashes_FEVT", 6743), ("r365537_Splashes_FEVT", 6538), ("r365537_Splashes_FEVT", 6744), ("r365537_Splashes_FEVT", 6539), ("r365537_Splashes_FEVT", 6803), ("r365537_Splashes_FEVT", 8387), ("r365537_Splashes_FEVT", 6602), ("r365537_Splashes_FEVT", 8184), ("r365537_Splashes_FEVT", 8121), ("r365537_Splashes_FEVT", 8007), ("r365537_Splashes_FEVT", 10316), ("r365537_Splashes_FEVT", 10317), ("r365537_Splashes_FEVT", 6676), ("r365537_Splashes_FEVT", 6464), ("r365537_Splashes_FEVT", 9176), ("r365537_Splashes_FEVT", 7783), ("r365537_Splashes_FEVT", 6746), ("r365537_Splashes_FEVT", 7949), ("r365537_Splashes_FEVT", 8123), ("r365537_Splashes_FEVT", 7245), ("r365537_Splashes_FEVT", 3794), ("r365537_Splashes_FEVT", 7835), ("r365537_Splashes_FEVT", 7618), ("r365537_Splashes_FEVT", 8008), ("r365537_Splashes_FEVT", 10387), ("r365537_Splashes_FEVT", 4110), ("r365537_Splashes_FEVT", 7886), ("r365537_Splashes_FEVT", 10431), ("r365537_Splashes_FEVT", 5744), ("r365537_Splashes_FEVT", 6745), ("r365537_Splashes_FEVT", 7573), ("r365537_Splashes_FEVT", 8185), ("r365537_Splashes_FEVT", 6465), ("r365537_Splashes_FEVT", 7836), ("r365537_Splashes_FEVT", 8122), ("r365537_Splashes_FEVT", 10432), ("r365537_Splashes_FEVT", 7136), ("r365537_Splashes_FEVT", 8553), ("r365537_Splashes_FEVT", 6603), ("r365537_Splashes_FEVT", 2221), ("r365537_Splashes_FEVT", 9335), ("r365537_Splashes_FEVT", 7088), ("r365537_Splashes_FEVT", 10045), ("r365537_Splashes_FEVT", 9177), ("r365537_Splashes_FEVT", 8790), ("r365537_Splashes_FEVT", 6466), ("r365537_Splashes_FEVT", 7089), ("r365537_Splashes_FEVT", 7887), ("r365537_Splashes_FEVT", 2299), ("r365537_Splashes_FEVT", 8009),
                         ] 

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
            'splash_sumq': hist.Hist(
                hist.axis.IntCategory([], name='event_number', label=r"Event Number", growth=True), 
                self._axis_sumq, 
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
                self._axis_depth7, 
                hist.axis.Variable(
                    10**np.linspace(0, 7, num=28+1, endpoint=True), 
                    name="sumq", 
                    label=r"Channel $\Sigma_{TS} q$ [fC]"
                )
            ),
            "splash_qtime": hist.Hist(
                hist.axis.IntCategory([], name='event_number', label=r"Event Number", growth=True), 
                self._axis_ieta, 
                self._axis_iphi, 
                self._axis_depth7, 
                hist.axis.Variable(
                    10**np.linspace(0, 7, num=28+1, endpoint=True), 
                    name="sumq", 
                    label=r"Channel $\Sigma_{TS} q$ [fC]"
                )
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

        output["event_sumq_dict"] = dict(zip(events["event"], event_sumq))

        runkey = events.metadata["dataset"]
        splash_mask = [(runkey, x) in self._splash_event_list for x in events["event"]]
        splash_events = events[splash_mask]
        for subdet in ["HB", "HE", "HF"]:
            splash_digis = splash_events[f"Digi{subdet}"]
            #splash_digis = splash_digis[splash_digis.valid]
            splash_digis["sumq"] = sum(splash_digis[f"fc{iTS}"] for iTS in range(nTS[subdet]))
            output["splash_depthmap"].fill(
                event_number = ak.flatten(ak.ones_like(splash_digis[splash_digis.valid]["ieta"]) * splash_events["event"]), 
                ieta = ak.flatten(splash_digis[splash_digis.valid]["ieta"]), 
                iphi = ak.flatten(splash_digis[splash_digis.valid]["iphi"]), 
                depth = ak.flatten(splash_digis[splash_digis.valid]["depth"]), 
                weight = ak.flatten(splash_digis[splash_digis.valid]["sumq"])
            )

            output["splash_sumq"].fill(
                event_number = ak.flatten(ak.ones_like(splash_digis[splash_digis.valid]["ieta"]) * splash_events["event"]), 
                sumq = ak.flatten(splash_digis[splash_digis.valid]["sumq"])
            )

        for subdet in ["HE", "HF"]:
            splash_digis = splash_events[f"Digi{subdet}"]
            splash_digis["sumq"] = sum(splash_digis[f"fc{iTS}"] for iTS in range(nTS[subdet]))
            #splash_digis["tdc"] = np.stack([ak.to_numpy(splash_digis[f"tdc{iTS}"]) for iTS in range(8)], axis=-1)
            for iTS in range(nTS[subdet]):
                splash_digis[f"tdctime{iTS}"] = ak.where(splash_digis[f"tdc{iTS}"] < 50, 
                    25 * iTS + 0.5 * splash_digis[f"tdc{iTS}"], 
                    np.nan * ak.ones_like(splash_digis[f"tdc{iTS}"]) # Fake large value
                    )
            splash_digis["tdctimestack"] = np.stack([ak.to_numpy(splash_digis[f"tdctime{iTS}"]) for iTS in range(nTS[subdet])], axis=-1)
            splash_digis["tdctime"] = np.min(splash_digis["tdctimestack"], axis=-1)

            output["splash_tdctime"].fill(
                event_number = ak.flatten(ak.ones_like(splash_digis[splash_digis.valid]["ieta"]) * splash_events["event"]),
                ieta = ak.flatten(splash_digis[splash_digis.valid]["ieta"]), 
                iphi = ak.flatten(splash_digis[splash_digis.valid]["iphi"]), 
                depth = ak.flatten(splash_digis[splash_digis.valid]["depth"]), 
                sumq = ak.flatten(splash_digis[splash_digis.valid]["sumq"]),
                weight = ak.flatten(splash_digis[splash_digis.valid]["tdctime"])
            )

        for subdet in ["HB", "HE", "HF"]:
            splash_digis = splash_events[f"Digi{subdet}"]
            splash_digis["sumq"] = sum(splash_digis[f"fc{iTS}"] for iTS in range(nTS[subdet]))
            splash_digis["qtime_sum"] = ak.zeros_like(splash_digis["fc0"])
            for iTS in range(nTS[subdet]):
                splash_digis["qtime_sum"] = splash_digis["qtime_sum"] + 25.0*iTS*splash_digis[f"fc{iTS}"]
            splash_digis["qtime"] = splash_digis["qtime_sum"] / splash_digis["sumq"]

            output["splash_qtime"].fill(
                event_number = ak.flatten(ak.ones_like(splash_digis[splash_digis.valid]["ieta"]) * splash_events["event"]),
                ieta = ak.flatten(splash_digis[splash_digis.valid]["ieta"]), 
                iphi = ak.flatten(splash_digis[splash_digis.valid]["iphi"]), 
                depth = ak.flatten(splash_digis[splash_digis.valid]["depth"]), 
                sumq = ak.flatten(splash_digis[splash_digis.valid]["sumq"]),
                weight = ak.flatten(splash_digis[splash_digis.valid]["qtime"])
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
