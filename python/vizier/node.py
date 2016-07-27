class Node():
    """
    Wrapper class to package required functionality for nodes
    """
    def __init__(self, name, deps, xform, group = None):
        self.name = name
        self.deps = deps
        self.group = group
        self.xform = xform
