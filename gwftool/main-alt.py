
import os
import sys
import json
import yaml
import time
import shutil
import logging
import argparse
import tempfile
import threading
import subprocess
import gwftool.warpdrive
import gwftool.tasks
import gwftool.runner

from gwftool.workflow_io import GalaxyWorkflow
from gwftool.tool_io import GalaxyTool, ToolBox


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

class WorkflowState:
    
    def __init__(self, workdir, basedir, inputs, workflow):
        self.inputs = inputs
        self.workflow = workflow
        self.results = {}
        self.states = {}
        
        self.workdir = workdir
        self.basedir = basedir
        self.job_num = 0
        
        self.running = {}
        
        for step in workflow.steps():
            if step.type == 'data_input':
                i = inputs[step.label]
                i['path'] = os.path.abspath(os.path.join(self.basedir, i['path']))
                self.results[ str(step.step_id) ] = { "output" : i }
            else:
                self.states[ str(step.step_id) ] = step.tool_state
                
    def missing_inputs(self, step):
        out = []
        for i in step.inputs:
            if i['name'] not in self.inputs:
                out.append(i['name'])
        return out

    def step_ready(self, step):
        ready = True
        for i in step.inputs:
            if i['name'] not in self.inputs:
                ready = False
        for name, conn in step.input_connections.items():
            if str(conn['id']) not in self.results:
                ready = False
        return ready
    
    def step_running(self, step):
        return str(step.step_id) in self.running
    
    def step_done(self, step):
        return str(step.step_id) in self.results
    
    def generate_outputs(self, step_id, tool):
        outputs = tool.get_outputs()
        out = {}
        outdir = os.path.join(self.workdir, str(step_id))
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        for name, data in tool.get_outputs().items():
            path = os.path.join("./", str(step_id), name)
            out[name] = { "class" : "File", "path" : os.path.abspath(os.path.join(self.workdir, path)) }
        return out
    
    def step_inputs(self, step_id):
        step_id = str(step_id)
        out = {}
        for k,v in self.states[step_id].items():
            if v is not None:
                out[k] = v
        for name, conn in self.workflow.get_step(step_id).input_connections.items():
            #print self.results[str(conn['id'])]
            conn_id = str(conn['id'])
            if self.workflow.get_step(conn_id).type == 'data_input':
                out[name] = self.results[conn_id]['output']
            else:
                out[name] = self.results[conn_id][conn['output_name']]
        out = expand_galaxy_input_dict(out)
        return out
    
    def add_outputs(self, step_id, outputs):
        step_id = str(step_id)
        self.results[step_id] = outputs
    
    def create_jobdir(self, step_id):
        self.job_num += 1
        j = os.path.abspath(os.path.join(self.workdir, "jobs", str(self.job_num)))
        os.mkdir(j)
        return j
    
    def run_job(self, step, tool):
        sinputs = self.step_inputs(step.step_id)
        outputs = self.generate_outputs(step.step_id, tool)
        job_dir = self.create_jobdir(step.step_id)
        script = tool.render_cmdline(sinputs, outputs)
        print "script (in %s): %s" % (job_dir, script)
        #print "step_inputs", sinputs
        r = Runner(tool, job_dir, script, sinputs, outputs)
        r.start()
        self.running[str(step.step_id)] = r
        
    
    def has_running(self):
        running = len(self.running) > 0
        #print "Running", len(self.running)
        cleanup = []
        for k, v in self.running.items():
            if not v.isAlive():
                cleanup.append(k)
                t_outputs = v.tool.get_outputs()
                for o, d in t_outputs.items():
                    if d.from_work_dir is not None:
                        src = os.path.abspath(os.path.join(v.jobdir, d.from_work_dir))
                        dst = v.outputs[o]['path'] 
                        print "mv %s %s" % (src, dst)
                        shutil.move(src, dst)
                self.add_outputs(k, v.outputs)
        for i in cleanup:
            del self.running[i]
        return running

class Runner(threading.Thread):
    def __init__(self, tool, jobdir, script, inputs, outputs):
        threading.Thread.__init__(self)
        self.tool = tool
        self.jobdir = jobdir
        self.script = script
        self.inputs = inputs
        self.outputs = outputs
    
    def run(self):
        docker_image = self.tool.get_docker_image()
        mounts = []
        print self.inputs
        for k, v in self.inputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                mounts.append("%s:%s:ro" % (v['path'], v['path']))
        for k, v in self.outputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                open(v['path'], "w").close()
                mounts.append("%s:%s" % (v['path'], v['path']))
                
        script_path = os.path.join(self.jobdir, "script")
        with open(script_path, "w") as handle:
            handle.write(self.script)
        mounts.append("%s:%s" % (self.jobdir, self.jobdir))
        mounts.append("%s:%s:ro" % (self.tool.tool_dir(), self.tool.tool_dir()))
        cmd = [which("docker"), "run", "--rm"]
        for i in mounts:
            cmd.extend(["-v", i])
        cmd.extend(["-w", self.jobdir])
        cmd.append(docker_image)
        cmd.append("bash")
        cmd.append(script_path)
        print "running", " ".join(cmd)
        proc = subprocess.Popen(cmd)
        proc.communicate()

def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tooldir", action="append", default=[])
    parser.add_argument("-w", "--workdir", default="./")
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    
    args = parser.parse_args(args)
    
    with open(args.inputs) as handle:
        inputs = yaml.load(handle.read())
    
    workdir = os.path.abspath(tempfile.mkdtemp(dir=args.workdir, prefix="gwftool_"))
    os.chmod(workdir, 0o777)
    
    toolbox = ToolBox()
    for d in args.tooldir:
        toolbox.scan_dir(d)
        
    print toolbox.keys()
    
    workflow = GalaxyWorkflow(ga_file=args.workflow)
    
    print "Workflow inputs: %s" % ",".join(workflow.get_inputs())
    os.mkdir(os.path.join(workdir, "jobs"))
    state = WorkflowState(workdir, os.path.dirname(args.inputs), inputs, workflow)
    
    for step in workflow.tool_steps():
        i = state.missing_inputs(step) 
        if len(i) > 0:
            raise Exception("Missing inputs: %s" % (",".join(i)))
    
    while True:
        ready_found = False
        for step in workflow.tool_steps():
            if state.step_ready(step) and not state.step_running(step) and not state.step_done(step):
                print "step", step.step_id, step.inputs, step.input_connections
                if step.tool_id not in toolbox:
                    raise Exception("Tool %s not found" % (step.tool_id))
                
                tool = toolbox[step.tool_id]
                state.run_job(step, tool)
                ready_found = True
        if not ready_found:
            if state.has_running():
                time.sleep(1)
            else:
                break
    
    for step in workflow.tool_steps():
        if not state.step_done(step):
            print "Not done", step, state.missing_inputs(step)
            #print state.results
            for name, conn in step.input_connections.items():
                if str(conn['id']) not in state.results:
                    print "not ready", conn

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
