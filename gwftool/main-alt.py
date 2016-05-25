
import os
import sys
import json
import yaml
import logging
import argparse
import tempfile
import threading
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
        
    def step_done(self, step):
        return str(step.step_id) in self.results
    
    def generate_outputs(self, step_id, tool):
        outputs = tool.get_outputs()
        out = {}
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
        return os.path.abspath(os.path.join(self.workdir, "jobs", str(self.job_num)))
    
    def run_job(self, step, tool):
        sinputs = self.step_inputs(step.step_id)
        outputs = self.generate_outputs(step.step_id, tool)
        job_dir = self.create_jobdir(step.step_id)
        shell, cmd = tool.render_cmdline(sinputs, outputs)
        print "cmd (in %s): %s" % (job_dir, cmd)
        
        r = Runner(job_dir, shell, cmd,
            lambda x: self.add_outputs(step.step_id, outputs)
        )
        r.start()
        #print "outputs", outputs
        
        t_outputs = tool.get_outputs()
        for o, d in t_outputs.items():
            if d.from_work_dir is not None:
                print "mv %s %s" % (os.path.abspath(os.path.join(job_dir, d.from_work_dir)), outputs[o]['path'] )
    
    def has_running(self):
        return False

class Runner(threading.Thread):
    def __init__(self, jobdir, shell, script, callback):
        threading.Thread.__init__(self)
        self.jobdir = jobdir
        self.shell = shell
        self.script = script
        self.callback = callback
    
    def run(self):
        print self.shell, self.script
        self.callback(0)

def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tooldir", action="append", default=[])
    parser.add_argument("-w", "--workdir", default="./")
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    
    args = parser.parse_args(args)
    
    with open(args.inputs) as handle:
        inputs = yaml.load(handle.read())
    
    #workdir = os.path.abspath(tempfile.mkdtemp(dir=args.workdir, prefix="gwftool_"))
    #os.chmod(workdir, 0o777)
    
    toolbox = ToolBox()
    for d in args.tooldir:
        toolbox.scan_dir(d)
        
    print toolbox.keys()
    
    workflow = GalaxyWorkflow(ga_file=args.workflow)
    
    print "Workflow inputs: %s" % ",".join(workflow.get_inputs())
    
    state = WorkflowState(args.workdir, os.path.dirname(args.workflow), inputs, workflow)
    
    for step in workflow.tool_steps():
        i = state.missing_inputs(step) 
        if len(i) > 0:
            raise Exception("Missing inputs: %s" % (",".join(i)))
    
    while True:
        ready_found = False
        for step in workflow.tool_steps():
            if state.step_ready(step) and not state.step_done(step):
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
