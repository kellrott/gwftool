
import re
import os
from glob import glob
from galaxy.tools.parser import get_tool_source
from Cheetah.Template import Template
from Cheetah.Filters import Filter


class ToolBox(object):
    def __init__(self, tool_dirs):
        self.config_files = {}
        self.tools = {}

        for tool_dir in tool_dirs:
            # scan through directory looking for tool_dir/*/*.xml files and
            # attempting to load them
            p1 = glob(os.path.join(os.path.abspath(tool_dir), "*.xml"))
            p2 = glob(os.path.join(os.path.abspath(tool_dir), "*", "*.xml"))

            for tool_conf in p1 + p2:
                tool = GalaxyTool(tool_conf)
                self.config_files[tool.tool_id] = tool_conf
                self.tools[tool.tool_id] = tool

    def keys(self):
        return self.tools.keys()

    def __contains__(self, key):
        return key in self.tools

    def __getitem__(self, key):
        return self.tools[key]


class ToolParam(object):
    def __init__(self, name, type, value=None, optional=False, label=""):
        self.name = name
        self.type = type
        self.value = value
        self.optional = optional
        self.label = label


class ToolOutput(object):
    def __init__(self, name, from_work_dir=None):
        self.name = name
        self.from_work_dir = from_work_dir


class CMDFilter(Filter):
    def filter(self, val, **kw):
        if isinstance(val, dict):
            if 'class' in val and val['class'] == 'File':
                return val['path']
        if isinstance(val, ToolOutput):
            return val.name
        return val


class GalaxyTool(object):
    def __init__(self, config_file):
        self.config_file = os.path.abspath(config_file)

        self.inputs = {}
        tool = get_tool_source(self.config_file)
        self.tool = tool

        self.tool_id = tool.parse_id()

        for p in tool.parse_input_pages().page_sources:
            for s in p.parse_input_sources():
                if s.input_type == "param":
                    name, param = self._parse_param(s)
                    self.inputs[name] = param
                elif s.input_type == "conditional":
                    prefix = s.get("name", "")
                    for q in s.parse_nested_input_source().parse_input_sources():
                        if q.input_type == "param":
                            name, param = self._parse_param(q, prefix)
                            self.inputs[name] = param

        self.outputs = {}
        outputs, _ = tool.parse_outputs("")
        for name, data in outputs.items():
            attrs = data.to_dict()
            from_work_dir = attrs.get('from_work_dir', '')
            self.outputs[name] = ToolOutput(name=name, from_work_dir=from_work_dir)

    def tool_dir(self):
        return os.path.abspath(os.path.dirname(self.config_file))

    def _parse_param(self, param_elem, prefix=None):
        type_ = param_elem.get("type")

        if type_ not in ['data', 'text', 'integer', 'float', 'boolean', 'select', 'hidden', 'baseurl', 'genomebuild', 'data_column', 'drill_down']:
            raise ValidationError('unknown input_type: %s' % (type_))

        name = param_elem.get("name")
        if prefix:
            name = prefix + "|" + name

        optional = param_elem.parse_optional()
        label = param_elem.parse_label()
        value = param_elem.get("value", "")
        param = ToolParam(name=name, type=type_, value=value, optional=optional, label=label)
        return name, param

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs
    
    def get_docker_image(self):
        reqs, containers = self.tool.parse_requirements_and_containers()
        if containers and containers[0].type == "docker":
            return containers[0].identifier
    
    def render_cmdline(self, inputs, outputs):
        cmd = self.tool.parse_command()

        temp = Template(cmd, searchList=[inputs, outputs], filter=CMDFilter)
        out = str(temp)
        out = out.replace("\n", " ").strip()

        inter = self.tool.parse_interpreter()
        if inter is not None:
            # TODO match what?
            res = re.search(r'^([^\s]+)(\s.*)$', out)
            print out
            spath = os.path.join(self.tool_dir(), res.group(1))
            if os.path.exists( spath ):
                out = spath + res.group(2)
            out = inter + " " + out

        return out
