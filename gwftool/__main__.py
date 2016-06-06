
import os
import sys
import json
import yaml
import logging
import argparse
import tempfile

from gwftool.workflow_io import GalaxyWorkflow
from gwftool.tool_io import GalaxyTool, ToolBox
from gwftool.engine import Engine



def main(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tooldir", action="append", default=[])
    parser.add_argument("-w", "--workdir", default="./")
    parser.add_argument("-o", "--outdir", default="./")
    parser.add_argument("--dryrun", default=False, action="store_true")
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    
    args = parser.parse_args(args)
    
    with open(args.inputs) as handle:
        inputs = yaml.load(handle.read())
    
    basedir = os.path.dirname(args.inputs)
    for i in inputs.values():
        if isinstance(i, dict) and i.get('class', None) == 'File':
            i['path'] = os.path.abspath(os.path.join(basedir, i['path']))
    if not args.dryrun:
        workdir = os.path.abspath(tempfile.mkdtemp(dir=args.workdir, prefix="gwftool_"))
        os.chmod(workdir, 0o777)
    else:
        workdir = None

    toolbox = ToolBox()
    for d in args.tooldir:
        toolbox.scan_dir(d)
        
    workflow = GalaxyWorkflow(ga_file=args.workflow)
    
    engine = Engine(workdir=workdir, outdir=args.outdir, toolbox=toolbox)
    engine.run_job(workflow, inputs, dryrun=args.dryrun)

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
