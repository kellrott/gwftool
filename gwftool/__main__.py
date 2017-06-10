
import os
import sys
import json
import yaml
import logging
import argparse
import tempfile

from gwftool.workflow_io import GalaxyWorkflow
from gwftool.tool_io import GalaxyTool, ToolBox
from gwftool.engine import Engine, NoNetLocalRunner


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tooldir", action="append", default=[])
    parser.add_argument("-w", "--workdir", default="./")
    parser.add_argument("-o", "--outdir", default="./")
    parser.add_argument("--no-net", action="store_true", default=False)
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    
    args = parser.parse_args(args)
    
    with open(args.inputs) as handle:
        inputs = yaml.load(handle.read())
    
    # Transform all input file paths to be absolute
    basedir = os.path.dirname(args.inputs)
    for i in inputs.values():
        if isinstance(i, dict) and i.get('class', None) == 'File':
            i['path'] = os.path.abspath(os.path.join(basedir, i['path']))

    # Get a temporary working directory
    workdir = os.path.abspath(tempfile.mkdtemp(dir=args.workdir, prefix="gwftool_"))
    os.chmod(workdir, 0o777)

    # Automatically include the directory containing the workflow file
    # in the list of tool directories.
    workflow_dir = os.path.dirname(args.workflow)
    if workflow_dir not in args.tooldir:
        args.tooldir.append(workflow_dir)

    # ToolBox provides access to all the galaxy tools in a the given tool directories.
    toolbox = ToolBox(args.tooldir)

    # GalaxyWorkflow represents the workflow definition file
    workflow = GalaxyWorkflow(ga_file=args.workflow)
    
    engine = Engine(args.outdir, workdir, toolbox, NoNetLocalRunner)
    engine.run_workflow(workflow, inputs)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
