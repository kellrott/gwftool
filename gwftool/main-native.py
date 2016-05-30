
import os
import sys
import json
import yaml
import logging
import argparse
import tempfile
import gwftool.warpdrive
import gwftool.tasks
import gwftool.runner
import gwftool.workflow_io


def which(program):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, program)
        if os.path.exists(p):
            return p

def main(args):
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--workdir", default="./")
    parser.add_argument("workflow")
    parser.add_argument("inputs")
    
    args = parser.parse_args(args)
    
    workdir = os.path.abspath(tempfile.mkdtemp(dir=args.workdir, prefix="gwftool_"))
    os.chmod(workdir, 0o777)
    
    with open(args.inputs) as handle:
        inputs = yaml.load(handle.read())
    
    engine = gwftool.runner.WorkflowRunner("/docstore")
    workflow = gwftool.workflow_io.GalaxyWorkflow(ga_file=args.inputs)    
    task = gwftool.tasks.GalaxyWorkflowTask(engine, workflow, inputs)

    task_path = os.path.join(workdir, "task")
    with open(task_path, "w") as handle:
        handle.write(json.dumps(task.to_dict()))

    docker_cmd = [which("docker"), "run"]
    docker_cmd.extend(["--rm", "-v", "%s:%s" % (workdir,"/gwftool")])
    docker_cmd.extend(["-p", "8080:8080"])
    if task.engine.get_docker_user():
        docker_cmd.extend(["-u", task.engine.get_docker_user()])
    docker_cmd.extend(["-v", "/var/run/docker.sock:/var/run/docker.sock"])
    docker_cmd.extend(["-v", "%s:/docstore" % (workdir)])
    if task.engine.get_work_volume():
        docker_cmd.extend(["-v", "%s" % task.engine.get_work_volume()])
    docker_cmd.append(task.engine.get_docker_image())
    docker_cmd.extend(task.engine.get_wrapper_command())
    docker_cmd.extend(["--docstore", "/docstore"])
    docker_cmd.append("/gwftool/task")
    logging.info("Running: %s", " ".join(docker_cmd))
    print("Running: %s" % " ".join(docker_cmd))
    
    #proc = subprocess.Popen( docker_cmd )
    #proc.communicate()

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
