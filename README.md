## hcalanalysis
This package contains some examples for analyzing hcalnano files (https://github.com/HCALPFG/HcalNano) using python-based columnar analysis tools. If you've never used coffea/uproot/awkward before, you might want to start with some tutorials:
   - [Columnar analysis HATS](https://indico.cern.ch/event/1042860/)
   - [Uproot/awkward array HATS](https://indico.cern.ch/event/1042866/)

### Setup
```
git clone git@github.com:HCALPFG/hcalanalysis
cd hcalanalysis
source firsttime.sh
```

### Test it out
Try running a processor:
```
cd hcalanalysis/example # Note: you should be in hcalanalysis/hcalanalysis
python ../run_processor.py testprocessor.TestProcessor \
   -y inputfiles.yaml \
   -o test.coffea \
   -w 4
```
(Or you can just `source local.sh`.)

In another terminal session, launch a notebook server:
```
cd hcalanalysis
source notebook.sh someNumberBiggerThan1024
```

In a third terminal, create an SSH tunnel for the notebooks:
```
ssh -N -L localhost:someNumberBiggerThan1024:localhost:someNumberBiggerThan1024 username@lxplusN.cern.ch
```
where `lxplusN` is the lxplus node running the notebook server. In your browser, navigate to the URL specified by the notebook server. Open `hcalanalysis/example/Plots.ipynb`, and run the notebook.

### Batch processing
As usual, any significant processing should be done on condor. See `hcalanalysis/example/condor.sh` for an example. 

### Tips
- hcalnano events are fairly large (1-2 MB / event). In the columnar analysis paradigm, the processor processes "chunks" of many events at once. Hence, the available memory limits the maximum chunk size to around 500 events.

