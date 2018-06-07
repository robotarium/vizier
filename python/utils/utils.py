

_get_response_types = {"data", "link", "stream"}
_status_codes = {1, 2, 3 4}
_methods = {"GET"}
_response_types = {"DATA", "STREAM"}
GetResponseTypeError = ValueError

#TODO: Delete this
def create_vizier_error_message(error_string):
    """Create a vizier error message for use when dependencies are not met, etc."""

    return {"type" : "ERROR", "message" : error_string}

def create_response(status, body, topic_type):
    """body: JSON dict (JSON-formatted dict to be transmitted with message)
    stats: int (status cod for response)
    -> JSON dict (in the vizier response format)"""

    return {"status":status, "body":body, "type":topic_type}

def create_response_channel(base, message_id):
    """Creates a response to a request message from the base channel and the message id. Base should be in the format <node_name>/responses.
    base: string (response channel base in format <node_name>/responses
    message_id: string (id of message to determine response channel)
    -> string (concatenated base and message id)"""

    return base + '/' + message_id

def create_request(request_id, method, uri, body):
    """
    Create vizier request message.
    id: string (unique identifier for request.  Should be something large and random)
    method: string (method for request: GET only for now)
    uri: string (URI for which request is made)
    body: JSON-formatted dict (optional body for request)
    -> JSON-formatted dict (containing the vizier request message)
    """
    return {"id":request_id, "method":method, "uri":uri, "body":body} 

#TODO: Delete this
def create_vizier_get_message(link, response_link):
    """
    Create a vizier get message.  The response to the get message will come on the response_link.

    link: string (link from which data is retrieved)
    response_link: string (link to which response is sent)
    """
    return {"type" : "GET", "link" : link, "response" : {"type" : "RESPONSE", "link" : response_link}}

#TODO: Delete this
def create_vizier_get_response(body, message_type="data"):
    """
    Create a response to a get message.  The response message can be data, a link, or a stream of data
    which looks identitcal to a link.  Thus, we add the types into the mix.
    """

    #Check if the supplied type is within the set of types allowed
    if(message_type not in _get_response_types):
        raise GetResponseTypeError("Type: " + repr(message_type) + " is not a valid get response type")

    return {"type" : message_type, "body" : body}

#TODO: Change this to something sane
def is_subset_of(superset, subset):
    """ Checks if subset is a subset of superset

    superset: string (separated by /)
    subset: string (separated by /)
    -> bool (indicating whether the relation holds)"""

    if(len(subset) == 0):
        return True

    superset_split = path.split('/')
    subset_split = link.split('/')

    for i in range(len(subset_split)):
        if(subset_split[i] != superset_split[i]):
            return False

    return True

#TODO: This is kind of bizarre as well.  Change this. Actually, probbaly just put this in the generate function.
def combine_paths(base, path):
    """Expands path to absolute or local

    base: string (base path in absolute format)
    path: string (path in absolute or relative format)
    -> string (path in absolute format)"""

    if(path[0] == '/'):
        return base + path
    else:
        return path

#TODO: Change this to reflect new descriptor files.  Namely, so many valid topics shouldn't be generated with the new format
def generate_links_from_descriptor(descriptor):
    """Recursively parses a descriptor file, expanding links as it goes.  This function will
    also check to ensure that all specified paths are valid, with respect to the local node.

   descriptor: dict (in the node descriptor format)
    -> dict (containing the non-recursively defined URIs)"""

    #TODO: Check if the type field is present to determine whether it's a valid link or not
    #TODO: Update to new node descriptor format.  Basically, don't worry about requests

    def parse_links(path, link, body):

        link = combine_paths(path, link)

        if not is_link_subset_of(path, link):
            print("Cannot have link that is not a subset of the current path")
            print("Link: " + link)
            print("Path: " + path)
            raise ValueError

        if(len(body["links"]) == 0):
            return {link : {"requests": body["requests"], "type": body["type"]}}

        #Else...

        local_links = {}
        for x in body["links"]:
            local_links.update(parse_links(link, x, body["links"][x]))

        return local_links

    # Start recursion with an empty base path, the endpoint name, and the descriptor
    return parse_links("", descriptor["end_point"], descriptor)
