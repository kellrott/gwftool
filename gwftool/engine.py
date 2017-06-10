
import os
import time
import json
import shutil
import shlex
import threading
import subprocess
from datetime import datetime
import gwftool.tes as tes


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
    def __init__(self, tool, id_, dir_, script, inputs, outputs):
        self.tool = tool
        self.id = id_
        self.dir = dir_
        self.script = script
        self.inputs = inputs
        self.outputs = outputs
        self.stdout = None
        self.stderr = None
        self.starttime = None
        self.endtime = None
        self.return_code = None

class AbstractRunner:
    def __init__(self, job):
        self.job = job
    def start(self):
        pass
    def isAlive(self):
        pass
        

class TESRunner:
    def __init__(self, job):
        self.job = job
        self.service = tes.TaskService("http://localhost:8000")

    def _get_inputs(self):
        inputs = []
        for k, v in self.job.inputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                inputs.append({
                    "url": v['path'],
                    "path": v['path'],
                })

        inputs.append({
            "path": "/opt/gwftool/script.sh",
            "contents": self.job.script,
        })

        inputs.append({
            "url": self.job.dir,
            "path": self.job.dir,
            "type": "DIRECTORY",
        })

        inputs.append({
            "url": self.job.tool.tool_dir(),
            "path": self.job.tool.tool_dir(),
            "type": "DIRECTORY",
        })
        return inputs

    def _get_outputs(self):
        outputs = []
        for k, v in self.job.outputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                outputs.append({
                    "url": v['path'],
                    "path": v['path'],
                })
        return outputs

    def start(self):
        self.task_id = self.service.create({
            "name": "TODO",
            "executors": [{
                    "image_name": self.job.tool.get_docker_image(),
                    "cmd": ["bash", "/opt/gwftool/script.sh"],
                    "workdir": self.job.dir,
            }],
            "inputs": self._get_inputs(),
            "outputs": self._get_outputs(),
        })

    def isAlive(self):
        task = self.service.get(self.task_id)
        state = task.get("state")
        print "STATE", state
        return not tes.done_state(state)


def NoNetLocalRunner(job):
    return LocalRunner(job, no_net=False)


class LocalRunner(threading.Thread):
    def __init__(self, job, no_net):
        threading.Thread.__init__(self)
        self.job = job
        self.no_net = no_net
    
    def run(self):
                
        # Write the command script to a file
        script_path = os.path.join(self.job.dir, "script")
        with open(script_path, "w") as handle:
            handle.write(self.job.script)

        # Build the docker volume mounts
        mounts = []

        for k, v in self.job.inputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                mounts.append("%s:%s:ro" % (v['path'], v['path']))

        for k, v in self.job.outputs.items():
            if isinstance(v, dict) and 'class' in v and v['class'] == 'File':
                open(v['path'], "w").close()
                mounts.append("%s:%s" % (v['path'], v['path']))

        mounts.append("%s:%s" % (self.job.dir, self.job.dir))
        mounts.append("%s:%s:ro" % (self.job.tool.tool_dir(), self.job.tool.tool_dir()))

        # Build docker run command
        cmd = [which("docker"), "run", "--rm"]
        if self.no_net:
            cmd.append("--net=none")

        for i in mounts:
            cmd.extend(["-v", i])

        cmd.extend(["-u", str(os.getuid())])
        cmd.extend(["-w", self.job.dir])
        docker_image = self.job.tool.get_docker_image()
        cmd.append(docker_image)
        cmd.append("bash")
        cmd.append(script_path)

        print "running", " ".join(cmd)

        self.job.starttime = datetime.now()

        # Execute local `docker run` command
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        self.job.return_code = proc.returncode
        self.job.stdout = stdout
        self.job.stderr = stderr
        self.job.endtime = datetime.now()


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

        outdir = os.path.join(self.outdir, step_id)
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        out = {}
        for name, _ in tool.get_outputs().items():
            path = os.path.join("./", step_id, name)
            out[name] = {
                "class": "File",
                "path": os.path.abspath(os.path.join(self.outdir, path))
            }
            print 'OUT', out
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
    
    def create_jobdir(self, step_id):
        self.job_num += 1
        j = os.path.abspath(os.path.join(self.workdir, "jobs", str(self.job_num)))
        os.mkdir(j)
        return j
    
    def _add_jobreport(self, job):
        meta_path = os.path.join(self.outdir, str(job.id), str(job.id) + ".json")
        with open(meta_path, "w") as handle:
            meta = {
                "stderr" : job.stderr,
                "stdout" : job.stdout,
                "script" : job.script,
                "image"  : job.tool.get_docker_image(),
                "tool"   : job.tool.tool_id,
                "exitcode" : job.return_code,
            }

            if job.starttime and job.endtime:
                meta["wallSeconds"] = (job.endtime - job.starttime).total_seconds()

            handle.write(json.dumps(meta))
    
    def _add_outputs(self, step_id, outputs):
        step_id = step_id
        self.results[step_id] = outputs
    
    def has_running(self):
        running = {}

        for k, v in self.running.items():
            if v.isAlive():
                running[k] = v
            else:
                for o, d in v.job.tool.get_outputs().items():
                    if d.from_work_dir is not None:
                        src = os.path.abspath(os.path.join(v.job.dir, d.from_work_dir))
                        dst = v.job.outputs[o]['path'] 

                        print "mv %s %s" % (src, dst)

                        if os.path.exists(src) and not os.path.exists(dst):
                            shutil.move(src, dst)
                        else:
                            print "Error: Missing output %s %s" % (k, src)

                self._add_jobreport(v.job)
                self._add_outputs(k, v.job.outputs)

        self.running = running
        return len(running) > 0



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

                    job = Job(
                        tool, step.step_id, job_dir,
                        script, inputs, outputs
                    )
                    r = self.new_runner(job)
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

