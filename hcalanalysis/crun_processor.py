'''
This script is a condor launcher for run_processor.py. Specifically, this script handles:
   - Dividing input files into condor subjobs
   - Creating usercode tarball for distribution to worker nodes
   - Setting up the executable (i.e. wrapper script around run_processor.py) to be run on the worker nodes
'''

import os
import sys
import datetime
import yaml
from pprint import pprint

import argparse
parser = argparse.ArgumentParser(description="Run hcalanalysis processors on condor")
parser.add_argument(dest="processor_name", type=str, help="Name of processor, i.e. bx1processor.BX1Processor")
parser.add_argument("-i", "--inputfiles", type=str, help="List of input files (comma-separated)")
parser.add_argument("-I", "--inputfilestxt", type=str, help="")
parser.add_argument("-y", "--inputfilesyaml", type=str, help="")
parser.add_argument("-d", "--dataset", type=str, help="Dataset name (only for -i/--inputfiles; for -I, the dataset name is taken from .yaml)")
parser.add_argument("-o", "--outputfile", type=str, required=True, help="Output file")
parser.add_argument("-w", "--workers", type=int, default=4, help="Number of workers per job")
parser.add_argument("--retar_venv", action="store_true", help="Retar venv (takes a while)")
parser.add_argument("--chunksize", type=int, default=250, help="Chunk size")
parser.add_argument("--maxchunks", type=int, default=None, help="Max chunks")
args = parser.parse_args()

# Input filehandling
fileset = {}
if args.inputfiles:
    fileset[args.dataset] = args.inputfiles.split(",")

elif args.inputfilestxt:
    fileset[args.dataset] = []
    with open(args.inputfilestxt, "r") as f:
        for line in f:
            fileset[args.dataset].append(line.strip())

elif args.inputfilesyaml:
    with open(args.inputfilesyaml) as f:
        doc = yaml.load(f)
        for dataset, filelist in doc.items():
            fileset[dataset] = filelist
pprint(fileset)

# Tar inputs
print("Tarring user code...")
whoami = os.getlogin()
tarball_dir = f"/afs/cern.ch/work/{whoami[0]}/{whoami}/tarballs"
os.system(f"mkdir -p {tarball_dir}")
os.system(f"tar -czvf {tarball_dir}/usercode.tar.gz -C $HCALANALYSISDIR/.. hcalanalysis/hcalanalysis hcalanalysis/hcaltools \
   --exclude='*.root' \
   --exclude='*.coffea*' \
   --exclude='*/job*' \
   --exclude='*.png' \
   --exclude='*.pdf' \
   --exclude='*.tar.gz' \
   --exclude='*ipynb ' \
   --exclude='hcalanalysis/hcalanalysis/condor'")
print("...done tarring user code")
if args.retar_venv:
    print("Retarring virtual environment...")
    os.system(f"tar -czf {tarball_dir}/venv.tar.gz -C $HCALANALYSISDIR/.. hcalanalysis/venv hcalanalysis/setup.py hcalanalysis/env.sh --exclude='*.root' --exclude='*.coffea' --exclude='*.png' --exclude='*.pdf'")
    print("...done tarring virtual environment")

# Make submission directory
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
submission_dir = os.path.expandvars(f"$HCALANALYSISDIR/hcalanalysis/condor/job{ts}")
os.system(f"mkdir -pv {submission_dir}")
cwd = os.getcwd()
os.chdir(submission_dir)

# Make resubmit script (calls resubmit for each dataset)
global_resubmit_script = open(f"{submission_dir}/resubmit_all.sh", "w")

# Loop over datasets
for dataset in fileset.keys():
    submission_subdir = f"{submission_dir}/{dataset}"
    os.system(f"mkdir -pv {submission_subdir}")
    os.chdir(submission_subdir)

    output_prefix = ".".join(args.outputfile.split(".")[:-1])
    output_suffix = args.outputfile.split(".")[-1]
    this_outputfile = f"{output_prefix}.{dataset}.{output_suffix}"

    # One job per file. 
    # @todo support >1 file per job? 
    # Might not be necessary, RAW files are already huge... typical hcalnano file size is already ~2GB. 
    this_nsubjobs = len(fileset[dataset])

    # Make run script (to be run on batch node)
    processor_command = f"python run_processor.py {args.processor_name} -i ${{INPUT_FILES[$1]}} -d {dataset} -o {this_outputfile}.$1 -w {args.workers}"
    if args.chunksize:
        processor_command += f" --chunksize {args.chunksize}"
    if args.maxchunks:
        processor_command += f" --maxchunks {args.maxchunks}"

    with open(f"{submission_subdir}/run.sh", "w") as run_script:
        run_script.write(f"""
#!/bin/bash
export HOME=$(pwd)
tar -xzf usercode.tar.gz
tar -xzf venv.tar.gz
echo 'After untarring, contents of ${{PWD}}:'
ls -lrth
cd hcalanalysis
source env.sh
cd hcalanalysis
echo 'Contents of ${{PWD}}:'
ls -lrth
INPUT_FILES=({" ".join(fileset[dataset])})
{processor_command}
mv *coffea* $_CONDOR_SCRATCH_DIR/
echo 'Done with run.sh'
""")

    # Files to transfer to batch
    files_to_transfer = [f"{tarball_dir}/usercode.tar.gz", f"{tarball_dir}/venv.tar.gz"]

    # Make condor command
    csub_command = f"csub {submission_subdir}/run.sh -F {','.join(files_to_transfer)} -n {this_nsubjobs} -d {submission_subdir} -t workday --mem 6000 2>&1"
    print(csub_command)
    with open(f"{submission_subdir}/csub_command.sh", "w") as csub_script:
        csub_script.write("#!/bin/bash\n")
        csub_script.write(csub_command + "\n")

    # Make resubmit command
    # - Lazy: rerun whole job if not all coffea outputs are present
    with open(f"{submission_subdir}/resubmit.sh", "w") as resubmit_script:
        resubmit_script.write(f"""
#!/bin/bash
# NSUCCESS=$(ls -l *coffea | wc -l)
NFAIL=$(find . -name "*coffea" -size -100c -exec stat -c "%s %n" {{}} \; | wc -l)
NTOTAL={this_nsubjobs}
#if [ \"$NSUCCESS\" -ne \"$NTOTAL\" ]; then
if [ \"$NFAIL\" -ge 1 ]; then 
    source csub_command.sh
fi
""")
    global_resubmit_script.write(f"cd {submission_subdir}\n")
    global_resubmit_script.write(f"source resubmit.sh\n")

    os.system(csub_command)

