import binascii
import os

# Global definitions for particular key names
_get_response_types = {'data', 'link', 'stream'}
_status_codes = {1, 2, 3, 4}
_methods = {'GET'}
_response_types = {'DATA', 'STREAM'}
_descriptor_keys = {'type': True, 'body': False}
GetResponseTypeError = ValueError


def create_message_id():
    return binascii.hexlify(os.urandom(20)).decode()


def create_response(status, body, topic_type):
    """body: JSON dict (JSON-formatted dict to be transmitted with message)
    stats: int (status cod for response)

    -> JSON dict (in the vizier response format)"""

    return {'status': status, 'body': body, 'type': topic_type}


def create_response_link(node, message_id):
    """Creates a response to a request message from the node's name and the message id.
    node: string (name of the node)
    message_id: string (id of message to determine response channel)

    -> string (concatenated base and message id)"""

    return '/'.join([node, 'responses', message_id])


def create_request_link(node):
    """Creates the appropriate request channel for a given node
    node: string (name of the node "end_point")

    -> string (the channel on which the request should be published"""

    return '/'.join([node, 'requests'])


def create_request(request_id, method, link, body):
    """Create vizier request message.
    id: string (unique identifier for request.  Should be something large and random)
    method: string (method for request: GET only for now)
    link: string (link for which request is made)
    body: JSON-formatted dict (optional body for request)

    -> JSON-formatted dict (containing the vizier request message)"""

    return {'id': request_id, 'method': method, 'link': link, 'body': body}


def is_subpath_of(superpath, subpath, delimiter='/'):
    """ Checks if subpath is a subpath of superpath
    superpath: string (separated by /)
    subpath: string (separated by /)

    -> bool (indicating whether the relation holds)"""

    # Special case
    if(subpath == ''):
        return True

    superpath_tokens = superpath.split(delimiter)
    subpath_tokens = subpath.split(delimiter)

    for i in range(len(subpath_tokens)):
        if(subpath_tokens[i] != superpath_tokens[i]):
            return False

    return True


def combine_paths(base, path):
    """Expands path to absolute or local
    base: string (base path in absolute format)
    path: string (path in absolute or relative format)

    -> string (path in absolute format)"""

    if(path[0] == '/'):
        return base + path
    else:
        return path


def extract_keys(descriptor):
    """Extract particular keys from a given descriptor file.  Namely, ignores the links attributes
    descriptor: dict (JSON-formatted for a node descriptor)

    -> dict (containing certain pre-specified keys to be extracted"""

    extracted = {}
    if('type' in descriptor):
        extracted['type'] = descriptor['type']
    else:
        raise ValueError

    # Initialize body to empty dict
    extracted['body'] = {}

    return extracted


def generate_links_from_descriptor(descriptor):
    """Recursively parses a descriptor file, expanding links as it goes.  This function will
    also check to ensure that all specified paths are valid, with respect to the local node.

   descriptor: dict (in the node descriptor format)
    -> dict (containing the non-recursively defined URIs)"""

    def parse_links(path, link, local_descriptor):
        """Sub function to be recursively called to process node descriptor"""

        link = combine_paths(path, link)

        if not is_subpath_of(link, path):
            # We want to ensure that this subset relation holds
            raise ValueError('Cannot have link (%s) that is not a subset of the current path (%s)' % (link, path))

        # If there are no more links from the current path, terminate the recursion and extract the relevant keys
        if('links' not in local_descriptor or len(local_descriptor['links']) == 0):
            if('type' in local_descriptor):
                return {link: extract_keys(local_descriptor)}

        # Else, recursively process all the links stemming from this one
        local_links = {}
        for x in local_descriptor['links']:
            local_links.update(parse_links(link, x, local_descriptor['links'][x]))
            # If the base path has a type attribute, include it as a valid link
            if('type' in local_descriptor):
                local_links[link] = extract_keys(local_descriptor)

        return local_links

    # Start recursion with an empty base path, the endpoint name, and the descriptor
    expanded_links = parse_links('', descriptor['end_point'], descriptor)

    requests = None
    # Ensure that requests are present and get them (if present)
    if('requests' in descriptor):
        requests = descriptor['requests']
    else:
        requests = []

    return expanded_links, descriptor['requests']
