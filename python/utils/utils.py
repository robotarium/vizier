

_get_response_types = {"data", "link", "stream"}
GetResponseTypeError = ValueError

def create_vizier_error_message(error_string):
    """
    Create a vizier error message for use when dependencies are not met, etc.
    """

    return {"type" : "ERROR", "message" : error_string}

def create_vizier_get_message(link, response_link):
    """
    Create a vizier get message.  The response to the get message will come on the response_link.
    """

    return {"type" : "GET", "link" : link, "response" : {"type" : "RESPONSE", "link" : response_link}}

def create_vizier_get_response(body, message_type="data"):
    """
    Create a response to a get message.  The response message can be data, a link, or a stream of data
    which looks identitcal to a link.  Thus, we add the types into the mix.
    """

    #Check if the supplied type is within the set of types allowed
    if(message_type not in _get_response_types):
        raise GetResponseTypeError("Type: " + repr(message_type) + " is not a valid get response type")

    return {"type" : message_type, "body" : body}

def is_link_subset_of(path, link):
    """ Checks if a link is a subset of the current path (directory wise) """

    if(len(path) == 0):
        return True

    path_tokens = path.split('/')
    link_tokens = link.split('/')

    for i in range(len(path_tokens)):
        if(link_tokens[i] != path_tokens[i]):
            return False

    return True

def expand_path(path, link):
    """
    Expands path to absolute or local
    """
    if(link[0] == '/'):
        return path + link
    else:
        return link


def generate_links_from_descriptor(descriptor):
    """
    Recursively parses a descriptor file, expanding links as it goes.  This function will
    also check to ensure that all specified paths are valid, with respect to the local node.
    """
    def parse_links(path, link, body):

        link = expand_path(path, link)

        if not is_link_subset_of(path, link):
            print("Cannot have link that is not a subset of the current path")
            print("Link: " + link)
            print("Path: " + path)
            raise ValueError

        if(len(body["links"]) == 0):
            return {link : body["requests"]}

        #Else...

        local_links = {}
        for x in body["links"]:
            local_links.update(parse_links(link, x, body["links"][x]))

            for request in body["links"][x]["requests"]:
                if("response" in request):
                    response_link = expand_path(expand_path(link, x), request["response"]["link"])
                    if not is_link_subset_of(link, response_link):
                        print("Cannot have link that is not a subset of the current path")
                        print("Link: " + response_link)
                        print("Path: " + link)
                        raise ValueError
                    request["response"]["link"] = response_link

        local_links.update({link : body["requests"]})
        return local_links

    return parse_links("", descriptor["end_point"], descriptor)
