
import os
import time
import json
import shutil
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


def NoNetLocalRunner(tool, jobid, jobdir, script, inputs, outputs):
    return Runner(tool, jobid, jobdir, script, inputs, outputs, no_net=False)


class Runner(threading.Thread):
    def __init__(self, tool, jobid, jobdir, script, inputs, outputs, no_net):
        threading.Thread.__init__(self)
        self.tool = tool
        self.jobid = jobid
        self.jobdir = jobdir
        self.script = script
        self.inputs = inputs
        self.outputs = outputs
        self.no_net = no_net
        self.stdout = None
        self.stderr = None
        self.starttime = None
        self.endtime = None
    
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
        if self.no_net:
            cmd.append("--net=none")
        for i in mounts:
            cmd.extend(["-v", i])
        cmd.extend(["-u", str(os.getuid())])
        cmd.extend(["-w", self.jobdir])
        cmd.append(docker_image)
        cmd.append("bash")
        cmd.append(script_path)
        print "running", " ".join(cmd)
        self.starttime=datetime.now()
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        self.return_code = proc.returncode
        self.stdout = stdout
        self.stderr = stderr
        self.endtime=datetime.now()


class WorkflowState:
    '''WorkflowState tracks the state of running jobs'''
    
    def __init__(self, outdir, workdir, inputs, workflow):
        self.inputs = inputs
        self.workflow = workflow
        self.results = {}
        self.states = {}
        
        self.outdir = outdir
        self.workdir = workdir
        self.job_num = 0
        
        self.running = {}
        
        # Pre-load the workflow inputs into the results dictionary.
        for step in workflow.steps():
            if step.type == 'data_input':
                i = inputs[step.label]
                self.results[step.step_id] = { "output" : i }
            else:
                self.states[step.step_id] = step.tool_state
                
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
            if conn['id'] not in self.results:
                ready = False
        return ready
    
    def step_running(self, step):
        '''step_running returns True if the given step is currently running'''
        return step.step_id in self.running
    
    def step_done(self, step):
        '''step_done returns True if the given step is done'''
        return step.step_id in self.results
    
    def generate_outputs(self, step_id, tool):
        step_id = str(step_id)
        outputs = tool.get_outputs()
        out = {}
        outdir = os.path.join(self.outdir, step_id)
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        for name, data in tool.get_outputs().items():
            path = os.path.join("./", step_id, name)
            out[name] = { "class" : "File", "path" : os.path.abspath(os.path.join(self.outdir, path)) }
        return out
    
    def step_inputs(self, step_id):
        step_id = step_id
        out = {}
        for k,v in self.states[step_id].items():
            if v is not None:
                out[k] = v
        for name, conn in self.workflow.get_step(step_id).input_connections.items():
            conn_id = conn['id']
            if self.workflow.get_step(conn_id).type == 'data_input':
                out[name] = self.results[conn_id]['output']
            else:
                out[name] = self.results[conn_id][conn['output_name']]
        out = expand_galaxy_input_dict(out)
        return out
    
    def add_outputs(self, step_id, outputs):
        step_id = step_id
        self.results[step_id] = outputs
    
    def create_jobdir(self, step_id):
        self.job_num += 1
        j = os.path.abspath(os.path.join(self.workdir, "jobs", str(self.job_num)))
        os.mkdir(j)
        return j
    
    def add_jobreport(self, job):
        meta_path = os.path.join(self.outdir, str(job.jobid) + ".json")
        with open(meta_path, "w") as handle:
            meta = {
                "stderr" : job.stderr,
                "stdout" : job.stdout,
                "script" : job.script,
                "image"  : job.tool.get_docker_image(),
                "tool"   : job.tool.tool_id,
                "exitcode" : job.return_code,
                "wallSeconds" : (job.endtime - job.starttime).total_seconds()
            }
            handle.write(json.dumps(meta))
    
    def has_running(self):
        running = len(self.running) > 0
        #print "Running", len(self.running)
        cleanup = []

        for k, v in self.running.items():
            if not v.isAlive():
                cleanup.append(k)

                for o, d in v.tool.get_outputs().items():
                    if d.from_work_dir is not None:
                        src = os.path.abspath(os.path.join(v.jobdir, d.from_work_dir))
                        dst = v.outputs[o]['path'] 
                        print "mv %s %s" % (src, dst)
                        if os.path.exists(src):
                            shutil.move(src, dst)
                        else:
                            print "Error: Missing output %s %s" % (k, src)

                self.add_jobreport(v)
                self.add_outputs(k, v.outputs)

        for i in cleanup:
            del self.running[i]

        return running



class Engine:

    def __init__(self, outdir, workdir, toolbox, runner_factory):
        self.workdir = os.path.abspath(workdir)
        self.outdir = os.path.abspath(outdir)
        self.toolbox = toolbox
        self.new_runner = runner_factory
    
    def run_workflow(self, workflow, inputs):
        print "Workflow inputs: %s" % ",".join(workflow.get_inputs())

        # initialize some working directories
        if not os.path.exists(self.workdir):
            os.mkdir(self.workdir)

        if not os.path.exists(self.outdir):
            os.mkdir(self.outdir)

        jobsdir = os.path.join(self.workdir, "jobs")
        if not os.path.exists(jobsdir):
            os.mkdir(jobsdir)

        # state tracks the state of running jobs
        state = WorkflowState(self.outdir, self.workdir, inputs, workflow)
        
        # Validate the inputs + workflow:
        # - check for missing inputs.
        # - check for tools in toolbox
        for step in workflow.tool_steps():
            i = state.missing_inputs(step) 
            if len(i) > 0:
                raise Exception("Missing inputs: %s" % (",".join(i)))
            if step.tool_id not in self.toolbox:
                raise Exception("Tool %s not found" % (step.tool_id))
        
        # Start looping over the workflow steps, running steps when they're ready.
        # Loop until the WorkflowState doesn't have any jobs running.
        while True:
            for step in workflow.tool_steps():
                # If the step is runnable (ready and not running nor done)
                if state.step_ready(step) and not state.step_running(step) \
                   and not state.step_done(step):

                    print "step", step.step_id, step.inputs, step.input_connections
                    tool = self.toolbox[step.tool_id]

                    inputs = state.step_inputs(step.step_id)
                    outputs = state.generate_outputs(step.step_id, tool)
                    job_dir = state.create_jobdir(step.step_id)
                    script = tool.render_cmdline(inputs, outputs)

                    print "script (in %s): %s" % (job_dir, script)

                    r = self.new_runner(
                        tool, step.step_id, job_dir,
                        script, inputs, outputs
                    )
                    r.start()
                    state.running[step.step_id] = r

            # If there are no jobs running, break, otherwise sleep and repeat.
            if not state.has_running():
                break
            else:
                time.sleep(1)
        
        # Check for unexpected things:
        # - steps that aren't done (they should all be done by now)
        for step in workflow.tool_steps():
            if not state.step_done(step):
                print "Not done", step, state.missing_inputs(step)
                #print state.results
                for name, conn in step.input_connections.items():
                    if conn['id'] not in state.results:
                        print "not ready", conn

