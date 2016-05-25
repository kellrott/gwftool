
import os
from glob import glob
from xml.dom.minidom import parse as parseXML
from Cheetah.Template import Template
from Cheetah.Filters import Filter
"""
Code for dealing with XML
"""

def getText(nodelist):
    rc = []
    if isinstance(nodelist, list):
        for node in nodelist:
            if node.nodeType in [ node.TEXT_NODE, node.CDATA_SECTION_NODE ]:
                rc.append(node.data)
    else:
        node = nodelist
        if node.nodeType in [ node.TEXT_NODE, node.CDATA_SECTION_NODE ]:
            rc.append(node.data)
    return ''.join(rc)


def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if child.localName == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                        yield out
                elif '*' == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                        yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )


class ToolBox(object):
    def __init__(self):
        self.config_files = {}
        self.tools = {}

    def scan_dir(self, tool_dir):
        #scan through directory looking for tool_dir/*/*.xml files and
        #attempting to load them
        for tool_conf in glob(os.path.join(os.path.abspath(tool_dir), "*.xml")) + glob(os.path.join(os.path.abspath(tool_dir), "*", "*.xml")):
            dom = parseXML(tool_conf)
            s = list(dom_scan(dom.childNodes[0], "tool"))
            if len(s):
                if 'id' in s[0][2]:
                    tool_id = s[0][2]['id']
                    self.config_files[tool_id] = tool_conf
                    self.tools[tool_id] = GalaxyTool(tool_conf)

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
        dom = parseXML(self.config_file)
        s = dom_scan(dom.childNodes[0], "tool/inputs/param")
        for elem, stack, attrs, text in s:
            for name, param in self._param_parse(elem):
                self.inputs[name] = param

        s = dom_scan(dom.childNodes[0], "tool/inputs/conditional")
        for elem, stack, attrs, text in s:
            c = list(dom_scan(elem, "conditional/param"))
            if 'name' in attrs:
                for p_elem, p_stack, p_attrs, p_text in c:
                    for name, param in self._param_parse(p_elem, prefix=attrs['name']):
                        self.inputs[name] = param

        self.outputs = {}
        s = dom_scan(dom.childNodes[0], "tool/outputs/data")
        for elem, stack, attrs, text in s:
            for name, data in self._data_parse(elem):
                self.outputs[name] = data


    def _param_parse(self, param_elem, prefix=None):
        if 'type' in param_elem.attributes.keys() and 'name' in param_elem.attributes.keys():
            param_name = param_elem.attributes['name'].value
            param_type = param_elem.attributes['type'].value
            if param_type in ['data', 'text', 'integer', 'float', 'boolean', 'select', 'hidden', 'baseurl', 'genomebuild', 'data_column', 'drill_down']:
                optional = False
                if "optional" in param_elem.attributes.keys():
                    optional = bool(param_elem.attributes.get("optional").value)
                label = ""
                if "label" in param_elem.attributes.keys():
                    label = param_elem.attributes.get("label").value
                value = ""
                if "value" in param_elem.attributes.keys():
                    value = param_elem.attributes.get("value").value
                param = ToolParam(name=param_name, type=param_type, value=value, optional=optional, label=label)
                if prefix is None:
                    yield (param_name, param)
                else:
                    yield (prefix + "|" + param_name, param)
            else:
                raise ValidationError('unknown input_type: %s' % (param_type))

    def _data_parse(self, data_elem, prefix=None):
        data_name = data_elem.attributes['name'].value
        print data_elem.attributes
        if data_elem.attributes.has_key('from_work_dir'):
            from_work_dir = data_elem.attributes['from_work_dir'].value
        else:
            from_work_dir = None        
        out = ToolOutput(name=data_name, from_work_dir=from_work_dir)
        yield data_name, out

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs
    
    def render_cmdline(self, inputs, outputs):
        t = None
        dom = parseXML(self.config_file)
        s = dom_scan(dom.childNodes[0], "tool/command")
        
        shell = "bash"        
        for elem, stack, attrs, text in s:
            if elem.attributes.has_key("interpreter"):
                shell = elem.attributes['interpreter'].value
            t = text
        temp = Template(t, searchList=[inputs, outputs], filter=CMDFilter)
        out = str(temp)
        out = out.replace("\n", " ").strip()
        return shell, out