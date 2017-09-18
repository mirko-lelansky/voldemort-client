from xml.dom import minidom
from voldemort_client.exception import ParserException

def get_child(element, name, required=True):
    children = [child for child in element.childNodes
            if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName == name]
    if not children:
        if required:
            raise ParserException("No child '%s' for element '%s'." % (name, element.nodeName))
        else:
            return None

    if len(children) > 1:
        raise ParserException("Multiple children '%s' for element '%s'." % (name, element.nodeName))
    return children[0]

def extract_text(element):
    """
    This method extracts the text content from the given node.

    :param element: the node to extract
    :type element: xml node
    """
    if element.nodeType == minidom.Node.TEXT_NODE:
        return element.data
    elif element.nodeType == minidom.Node.ELEMENT_NODE:
        text = ""
        for child in element.childNodes:
            text += extract_text(child)
        return text

def extract_childnode_text(element, name, required = True, default = None):
    """
    This method fetches the a child node with the specific name and returns the
    content.

    :param element: the root node
    :type element: xml node
    :param name: the name of the child node
    :type name: str
    :param required: signals if the child node is required
    :type required: bool
    :param default: the default value
    :type default: object
    """
    if default:
        required = False

    child = get_child(element, name, required = required)
    if not child:
        return default

    return extract_text(child)

def int_or_none(element):
    if element is None:
        return element
    else:
        return int(element)
