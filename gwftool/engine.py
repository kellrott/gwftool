
import os
from os.path import abspath
import time
import logging
import shutil
import shlex
import threading
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("gwftool")

def which(program):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, program)
        if os.path.exists(p):
            return p


def expand_galaxy_input_dict(val):
    """
    takes a galaxy input dict, which has "|" delimited
    fields (param1|data1|file) to be a fully fleshed out 
    galaxy json record
    """
    out = {}
    for k, v in val.items():
        out[k] = v
    for k, v in val.items():
        kl = k.split("|")
        if len(kl) > 1:
            o = out
            for kli in kl[:-1]:
                if kli not in o:
                    o[kli] = {}
                o = o[kli]
            o[kl[-1]] = v
    return out


class Step:
    def __init__(self, id_, dir_):
        self.id = id_
        self.dir = dir_
        self.tool = None
        self.script = ""
        self.inputs = {}
        self.outputs = {}

    @property
    def stdout(self):
        return os.path.join(self.dir, "stdout")

    @property
    def stderr(self):
        return os.path.join(self.dir, "stderr")


def to_TES_task(step):
    """
    Given a Step instance, convert it into a TES task structure.
    """
    # prefix all inputs with the absolute path of the step working directory.
    inputs = []
    for k, v in step.inputs.items():
        if isinstance(v, dict) and "class" in v and v["class"] == "File":
            inputs.append({
                "url": v["path"],
                "path": v["path"],
            })

    inputs.append({
        "path": "/opt/gwftool/script.sh",
        "contents": step.script,
    })

    # TODO look into why this is required.
    inputs.append({
        "url": step.tool.tool_dir(),
        "path": step.tool.tool_dir(),
        "type": "DIRECTORY",
    })

    # prefix all outputs with the absolute path of the step working directory.
    outputs = []
    for k, v in step.outputs.items():
        if isinstance(v, dict) and "class" in v and v["class"] == "File":
            outputs.append({
                "url": v["url"],
                "path": v["path"],
            })

    outputs.append({
        "url": step.dir,
        "path": step.dir,
        "type": "DIRECTORY",
    })


    task = {
        "name": "TODO Name",
        "executors": [{
                "image_name": step.tool.get_docker_image(),
                "cmd": ["bash", "/opt/gwftool/script.sh"],
                "workdir": step.dir,
                "stdout": step.stdout,
                "stderr": step.stderr,
        }],
        "inputs": inputs,
        "outputs": outputs,
    }
    return task



def resolve_workflow(toolbox, workdir, workflow, workflow_inputs):
    """
    resolve_workflow is responsible for loading a workflow file and associated
    tools, resolving the step inputs and outputs in file system paths,
    and converting those steps into TES tasks.

    resolve_workflow returns a list of TES task structures.

    "toolbox" provides access to a tool's XML descriptor and dependencies.
    This should be an instance of tool_io.ToolBox.

    "workdir" is the path to the working directory, where all steps
    will run, and input/output files will be stored.

    "workflow" is a Galaxy workflow structure.
    This should be an instance of workflow_io.GalaxyWorkflow.

    "workflow_inputs" is a dictionary describing the workflow inputs.
    See examples/md5_sum/input.json

    Currently this only supports local filesystems.
    """

    # Tracks workflow inputs
    data_input = {}
    # Tracks steps by ID, so that their inputs/outputs can be connected.
    steps = {}
    workdir = abspath(workdir)

    # Resolving the steps is done in two passes:
    # 1. create Step objects and generate input/output paths for each.
    #    index these steps by ID, so they can be connected in the next pass.
    #
    # 2. connect the inputs of steps to the outputs of previous steps.

    # First pass. Create Step objects and index the steps by ID.
    for s in workflow.steps():

        # Galaxy workflows define their inputs as steps, which are denoted
        # with the type "data_input". In that case, map the ID to the
        # given workflow_inputs dict, then skip the rest.
        if s.type == "data_input":
            data_input[s.step_id] = workflow_inputs[s.label]
            continue

        step = Step(s.step_id, os.path.join(workdir, str(s.step_id)))
        steps[s.step_id] = step

        step.tool = toolbox.get(s.tool_id)
        # Check for unknown tool.
        if not step.tool:
            raise Exception("Tool %s not found" % (s.tool_id))

        # Map tool_io.ToolOutput into step.outputs.
        for name, out in step.tool.get_outputs().items():
            # Tools may use "from_work_dir" to specify the name of the
            # file as it's written by the tool, inside the container.
            container_file_name = out.name
            if out.from_work_dir:
                container_file_name = out.from_work_dir

            step.outputs[name] = {
                # TODO related to tool_io.CMDFilter
                "class": "File",
                "path": os.path.join(step.dir, container_file_name),
                "url": os.path.join(step.dir, out.name),
            }

    # Second pass. Connect the input/output files between steps.
    for s in workflow.tool_steps():
    
        # Check for missing inputs.
        missing = []
        for i in s.inputs:
            if i["name"] not in workflow_inputs:
                missing.append(i["name"])

        if len(missing) > 0:
            raise Exception("Missing inputs: %s" % (",".join(missing)))

        step = steps[s.step_id]
        step.inputs = dict(s.tool_state)

        for name, conn in s.input_connections.items():
            conn_id = conn["id"]
            if conn_id in data_input:
                step.inputs[name] = data_input[conn_id]
            else:
                step.inputs[name] = steps[conn_id].outputs[conn["output_name"]]

        step.inputs = expand_galaxy_input_dict(step.inputs)
        # Render the command template into a script.
        step.script = step.tool.render_cmdline(step.inputs, step.outputs)

    # Convert the steps to TES task structures.
    tasks = []
    for step in steps.values():
        t = to_TES_task(step)
        tasks.append(t)

    return tasks
