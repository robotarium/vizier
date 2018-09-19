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
    """Creates a unique message id for a request.

    Returns:
        A secure, random 64-byte message ID

    """

    return binascii.hexlify(os.urandom(20)).decode()


def create_response(status, body, topic_type):
    """Creates a response message suitable for the vizier network.

    Args:
        status (int): Status integer for response

    Returns:
        A JSON-formatted dict representing the response message

    """

    return {'status': status, 'body': body, 'type': topic_type}


def create_response_link(node, message_id):
    """Creates a response to a request message from the node's name and the message id.

    Args:
        node (str):  Name of the node for the response link.  Format will be <node_name>/responses/message_id
        message_id (str): Unique ID of message for response

    Returns:
        String of the form <node_name>/responses/message_id on which the response should be published

    """

    return '/'.join([node, 'responses', message_id])


def create_request_link(node):
    """Creates the appropriate request channel for a given node

    Args:
        node (str):  Name of the node on which the request link should be created

    Returns:
        String representing the request link.  It is of the form <node_name>/requests

    """

    return '/'.join([node, 'requests'])


def create_request(request_id, method, link, body):
    """Create vizier request message.

    Args:
        id (str):  Unique identifier for request.  Ideally, should be somethign large and random
        method (str):  Method for request (e.g., GET)
        link (str):  Link on which request is made
        body (dict): JSON-formatted dict containing the body for the request (could be optional)

    Returns:
        JSON-formatted dict respresenting the request message.  This message can be published on the requests channel

    """

    return {'id': request_id, 'method': method, 'link': link, 'body': body}


def is_subpath_of(superpath, subpath, delimiter='/'):
    """Checks if subpath is a subpath of superpath

    Args:
        superpath (str): A string-based path where values are separated by '/'
        subpath (str):  A string-based path where values are separated by '/'
    subpath: string (separated by /)

    Returns:
        A bool indicating whether the relation holds

    """

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

    If the path is in absolute format, doesn't do anything.  Otherwise, appends the base path
    to the path for form an absolute path

    Args:
        base (str): Base path in absolute format
        path (str): Path in absolute or relative format

    Returns:
        A string in absolute format

    """

    if(path[0] == '/'):
        return base + path
    else:
        return path


def extract_keys(descriptor):
    """Extract particular keys from a given descriptor file.  Namely, ignores the links attributes

    Args:
        descriptor (dict): JSON-formmated descriptor for a particular link

    Returns:
        A dict containing a mapping of keys to be extracted.  For example

        .. code-block:: python

            {'type': 'DATA', 'body': {}}

    """

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

    The descriptor is a JSON-formatted dict.  For example,

    .. code-block:: python

        {
            'end_point': 'test_node',
            'links':
            {
                'sub_link':
                {
                    'type': 'DATA
                }
            },
            'requests': []
        }

    Args:
        descriptor (dict):  A JSON-formatted descriptor for the node.

    Returns:
        A dict containing the non-recursively defined links, and the requests of the node.  For example

    Raises:
        ValueError: If the recursive definition of the link paths is invalid.

    """

    def parse_links(path, link, local_descriptor):
        """Sub function to be recursively called to process node descriptor

        Args:
            link (str): Link that is currently being parsed
            local_descriptor (dict): Descriptor for link that is currently being parsed

        Returns:
            A dict representing the recursively parsed descriptors from this link down

        Raises:
            ValueError:  If the recursive definition of the link paths is invalid
        """

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

    # Parse out required vs. optional requests.  Requests formatted as a string are assumed to be optional.  Those formatted as a dict are the most general
    # and the type defaults to the Boolean value of the 'required' field
    requests_d = {}
    for x in requests:
        # Check optional keys
        if('required' not in x):
            # optional key is included
            x['required'] = False

        # Put into dictionary rather than list form
        if('link' in x):
            requests_d[x['link']] = x
        else:
            raise ValueError('Request type must be in proper format')

    return expanded_links, requests_d
