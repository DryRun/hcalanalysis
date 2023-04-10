## hcalanalysis
This package contains some examples for analyzing hcalnano files (https://github.com/HCALPFG/HcalNano) using python-based columnar analysis tools. If you've never used coffea/uproot/awkward before, you might want to start with some tutorials:
   - [Columnar analysis HATS](https://indico.cern.ch/event/1042860/)
   - [Uproot/awkward array HATS](https://indico.cern.ch/event/1042866/)

### Setup
```
git clone git@github.com:DryRun/hcalanalysis
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

Next, we plot the histograms using a Jupyter notebook. We'll launch the Jupyter server on lxplus, and setup an SSH tunnel to lxplus so we can make the plots in a (local) browser window. This can be done in one SSH command:

```
export PORT=7778
export USERNAME=dryu
export LXPLUSFOLDER=HCAL/hcalanalysis
ssh -L localhost:${PORT}:localhost:${PORT} ${USERNAME}@lxplus.cern.ch "cd ${LXPLUSFOLDER}; echo 'Setting up env...'; source env.sh; echo 'Launching notebook...'; source notebookserver.sh ${PORT};"
```

(These commands are provided in the script `lxplusnb.sh`, so you might want to checkout the repository on your local machine, and edit+run the script.)

Once the Jupyter server is up and running, navigate to the URL specified in the terminal window. Open `hcalanalysis/example/Plots.ipynb`, and run the notebook.

### Batch processing
As usual, any significant processing should be done on condor. See `hcalanalysis/example/condor.sh` for an example. 

### Tips
- hcalnano events are fairly large (1-2 MB / event). In the columnar analysis paradigm, the processor processes "chunks" of many events at once. Hence, the available memory limits the maximum chunk size to around 500 events.

