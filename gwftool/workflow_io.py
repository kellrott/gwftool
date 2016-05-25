

import json

class WorkflowStep(object):
    def __init__(self, workflow, desc):
        self.workflow = workflow
        self.desc = desc
        self.step_id = desc["id"]
        self.uuid = desc['uuid']
        self.type = self.desc['type']
        label = str(self.desc['uuid'])
        if self.desc['label'] is not None:
            label = self.desc['label']
        elif len(self.desc['annotation']):
            label = self.desc['annotation']            
        self.label = label
        self.tool_id = self.desc.get('tool_id', None)
        state = json.loads(self.desc.get('tool_state', "null"))
        self.tool_state = {}
        if self.type == "tool":
            for k, v in state.items():
                if k not in ["__page__", "__rerun_remap_job_id__"]:
                    self.tool_state[k] = json.loads(v)
        elif self.type == "data_input":
            self.tool_state['name'] = state['name']
            self.label = state['name']
        self.input_connections = self.desc.get("input_connections", {})
        self.inputs = self.desc.get("inputs", [])
        self.outputs = self.desc.get("outputs", [])
        self.annotation = self.desc.get("annotation", "")

    def validate_input(self, data, tool):
        tool_inputs = tool.get_inputs()
        for tin in tool_inputs:
            value = None
            tin_state = self.find_state(tin)
            if tin_state is not None:
                value = tin_state
            if tool_inputs[tin].type == 'data':
                if value is None:
                    if tin not in self.input_connections:
                        if not tool_inputs[tin].optional:
                            raise ValidationError("Tool %s Missing input dataset: %s.%s" % (self.tool_id, self.step_id, tin))
            else:
                if value is None:
                    if self.step_id not in data['ds_map'] or tin not in data[self.step_id]:
                        if tool_inputs[tin].value is None and not tool_inputs[tin].optional:
                            raise ValidationError("Tool %s Missing input: %s.%s" % (self.tool_id, self.step_id, tin))
                else:
                    if isinstance(value, dict):
                        #if they have missed one of the required runtime values in the pipeline
                        if value.get("__class__", None) == 'RuntimeValue':
                            if self.step_id not in data['parameters'] or tin not in data['parameters'][self.step_id]:
                                raise ValidationError("Tool %s Missing runtime value: %s.%s" % (self.tool_id, self.step_id, tin))

    def find_state(self, param):
        if param.count("|"):
            return self.find_state_rec(param.split("|"), self.tool_state)
        return self.tool_state.get(param, None)

    def find_state_rec(self, params, state):
        if len(params) == 1:
            return state[params[0]]
        if params[0] not in state:
            return None
        return self.find_state_rec(params[1:],state[params[0]])

class ValidationError(Exception):

    def __init__(self, message):
        super(ValidationError, self).__init__(message)

class GalaxyWorkflow(object):
    """
    Document describing Galaxy Workflow
    """
    def __init__(self, workflow=None, ga_file=None):
        if ga_file is not None:
            with open(ga_file) as handle:
                self.desc = json.loads(handle.read())
        else:
            self.desc = workflow

    def to_dict(self):
        return self.desc

    def steps(self):
        for s in self.desc['steps'].values():
            yield WorkflowStep(self, s)
    
    def get_step(self, step_id):
        step_id = str(step_id)
        return WorkflowStep(self, self.desc['steps'][step_id])

    def tool_steps(self):
        for s in self.desc['steps'].values():
            if s['type'] == 'tool':
                yield WorkflowStep(self, s)


    def get_inputs(self):
        inputs = []
        for step in self.steps():
            if step.type == 'data_input':
                inputs.append(step.label)
        return inputs

    def get_outputs(self, all=False):
        outputs = []
        hidden = self.get_hidden_outputs()
        for step in self.steps():
            if step.type == 'tool':
                for o in step.outputs:
                    output_name = "%s|%s" % (step.label, o['name'])
                    if all or output_name not in hidden:
                        outputs.append( output_name )
        return outputs

    def get_hidden_outputs(self):
        outputs = []
        for step in self.steps():
            if step.type == 'tool' and 'post_job_actions' in step.desc:
                for pja in step.desc['post_job_actions'].values():
                    if pja['action_type'] == 'HideDatasetAction':
                        outputs.append( "%s|%s" % (step.label, pja['output_name']) )
        return outputs

    def validate_input(self, data, toolbox):
        for step in self.steps():
            if step.type == 'tool':
                if step.tool_id not in toolbox:
                    raise ValidationError("Missing Tool: %s" % (step.tool_id))
                tool = toolbox[step.tool_id]
                step.validate_input(data, tool)
            if step.type == 'data_input':
                if step.step_id not in data['ds_map']:
                    raise ValidationError("Missing Data Input: %s" % (step.inputs[0]['name']))
        return True

    def adjust_input(self, input):
        dsmap = {}
        parameters = {}
        out = {}
        for k, v in input.get("inputs", input.get("ds_map", {})).items():
            if k in self.desc['steps']:
                out[k] = v
            else:
                found = False
                for step in self.steps():
                    label = step.uuid
                    if step.type == 'data_input':
                        if step.inputs[0]['name'] == k:
                            found = True
                            dsmap[label] = {'src':'uuid', 'id' : v.uuid}

        for k, v in input.get("parameters", {}).items():
            if k in self.desc['steps']:
                out[k] = v
            else:
                #found = False
                for step in self.steps():
                    label = step.uuid
                    if step.type == 'tool':
                        if step.annotation == k:
                            #found = True
                            parameters[label] = v

        #TAGS
        for tag in input.get("tags", []):
            for step, step_info in self.desc['steps'].items():
                step_name = step_info['uuid']
                if step_info['type'] == "tool":
                    pja_map = {}
                    for i, output in enumerate(step_info['outputs']):
                        output_name = output['name']
                        pja_map["RenameDatasetActionout_file%s" % (i)] = {
                            "action_type" : "TagDatasetAction",
                            "output_name" : output_name,
                            "action_arguments" : {
                                "tags" : tag
                            },
                        }
                    if step_name not in parameters:
                        parameters[step_name] = {} # json.loads(step_info['tool_state'])
                    parameters[step_name]["__POST_JOB_ACTIONS__"] = pja_map
        out['workflow_id'] = self.desc['uuid']
        out['inputs'] = dsmap
        out['parameters'] = parameters
        out['inputs_by'] = "step_uuid"
        return out

