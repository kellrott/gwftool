
import os
import sys
import json
import yaml
import logging
import argparse
import tempfile

import gwftool.workflow_io as workflow_io
import gwftool.tool_io as tool_io
import gwftool.engine as engine


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
    toolbox = tool_io.ToolBox(args.tooldir)

    # GalaxyWorkflow represents the workflow definition file
    workflow = workflow_io.GalaxyWorkflow(ga_file=args.workflow)
    
    # Convert the workflow steps into TES task structures
    tasks = engine.resolve_workflow(toolbox, workdir, workflow, inputs)

    # Write the TES tasks to a series of files.
    for i, t in enumerate(tasks):
        path = "task-{}.json".format(i)
        with open(path, "w") as fh:
            json.dump(t, fh, indent=4, sort_keys=True)



if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
