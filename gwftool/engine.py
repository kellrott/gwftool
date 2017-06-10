
import os
import time
import json
import shutil
import shlex
import threading
import subprocess
from datetime import datetime


def which(program):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, program)
        if os.path.exists(p):
            return p


def expand_galaxy_input_dict(val):
    """
    takes a galaxy input dict, which has '|' delimited
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


class Job:
    def __init__(self, id_):
        self.id = id_
        self.dir = "TODO JOB DIR"
        self.tool = None
        self.script = ""
        self.inputs = {}
        self.outputs = {}


def to_TES_task(job):
    inputs = []
    for k, v in job.inputs.items():
        if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
            inputs.append({
                "url": v['path'],
                "path": v['path'],
            })

    inputs.append({
        "path": "/opt/gwftool/script.sh",
        "contents": job.script,
    })

    inputs.append({
        "url": job.dir,
        "path": job.dir,
        "type": "DIRECTORY",
    })

    inputs.append({
        "url": job.tool.tool_dir(),
        "path": job.tool.tool_dir(),
        "type": "DIRECTORY",
    })

    outputs = []
    for k, v in job.outputs.items():
        if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
            outputs.append({
                "url": v['path'],
                "path": v['path'],
            })

    task = {
        "name": "TODO",
        "executors": [{
                "image_name": job.tool.get_docker_image(),
                "cmd": ["bash", "/opt/gwftool/script.sh"],
                "workdir": job.dir,
        }],
        "inputs": inputs,
        "outputs": outputs,
    }
    return task


class Engine:

    def __init__(self, toolbox):
        self.toolbox = toolbox
    
    def run_workflow(self, workflow, workflow_inputs):
        print "Workflow inputs: %s" % ",".join(workflow.get_inputs())

        data_input = {}
        jobs = {}

        for step in workflow.steps():
            print 'step', step.type
            if step.type == 'data_input':
                data_input[step.step_id] = workflow_inputs[step.label]
                continue

            job = Job(step.step_id)
            jobs[step.step_id] = job

            job.tool = self.toolbox.get(step.tool_id)
            # Check for unknown tool.
            if not job.tool:
                raise Exception("Tool %s not found" % (step.tool_id))

            for name, _ in job.tool.get_outputs().items():
                job.outputs[name] = {
                    "class": "File",
                    "path": str(step.step_id) + "/" + name,
                }

        print jobs
        print data_input

        for step in workflow.tool_steps():
        
            # Check for missing inputs.
            missing = []
            for i in step.inputs:
                if i['name'] not in workflow_inputs:
                    missing.append(i['name'])

            if len(missing) > 0:
                raise Exception("Missing inputs: %s" % (",".join(missing)))

            job = jobs[step.step_id]
            job.inputs = dict(step.tool_state)

            for name, conn in step.input_connections.items():
                conn_id = conn['id']
                if conn_id in data_input:
                    job.inputs[name] = data_input[conn_id]
                else:
                    job.inputs[name] = jobs[conn_id].outputs[conn['output_name']]

            job.inputs = expand_galaxy_input_dict(job.inputs)
            job.script = job.tool.render_cmdline(job.inputs, job.outputs)

        for job in jobs.values():
            print json.dumps(to_TES_task(job), indent=4)
