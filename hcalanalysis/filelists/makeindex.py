import os
import subprocess
import json

import argparse
parser = argparse.ArgumentParser(description="""
Index a folder on EOS and dump to json
[1] LFN = logical file name, which is a path starting with /store/group/dpg_hcal/...
""")
parser.add_argument("-d", "--directory", type=str, required=True, help="Directory to index")
parser.add_argument("-k", "--key", type=str, required=True, help="Key of this dataset in the dictionary")
parser.add_argument("-e", "--ext", default=".root", type=str, help="File extension to index")
args = parser.parse_args()

def get_children(parent):
	#print(f"DEBUG : Call to get_children({parent})")
	command = f"eos root://eoscms.cern.ch ls -F {parent}"
	print(command)
	result = subprocess.getoutput(command)#, stdout=subprocess.PIPE)
	print(result)
	return result.split("\n")

def get_subfolders(parent):
	subfolders = []
	for x in get_children(parent):
		if len(x) == 0: 
			continue
		if x[-1] == "/":
			subfolders.append(x)
	return subfolders

folder_contents = get_children(args.directory)
folder_contents = [x for x in folder_contents if x.endswith(args.ext)]
folder_contents = [f"root://eoscms.cern.ch/{args.directory}/{x}" for x in folder_contents]
index = {args.key: folder_contents}
with open(f"nanoindex/nanoindex_{args.key}.json", "w") as f:
	json.dump(index, f, sort_keys=True, indent=2)
