import time
import json
import argparse
import vizier.node as node
import unittest

class TestNode(unittest.TestCase):
    def setUp(self):
        
        # Ensure that we can open the nodes file
        filepath = '../config/node_desc_a.json'   

        try:
            f = open(filepath, 'r')
            node_descriptor = json.load(f)
            f.close()
        except Exception as e:
            print(repr(e))
            print('Could not open given node file {}'.format(filepath))
            return -1
    
        self.node = node.Node('localhost', 1883, node_descriptor)
        self.node.start()
    
    def test_publishable_topics(self): 
        self.assertEqual(self.node.publishable_links, {'a/a_sub'})    

    def test_subscribable_topics(self):
        self.assertEqual(self.node.subscribable_links, set())

    def test_gettable_topics(self):
        self.assertEqual(self.node.gettable_links, set())

    def test_puttable_topics(self):
        print(self.node.puttable_links)
        self.assertEqual(self.node.puttable_links, set())

    def tearDown(self):
        self.node.stop()

#def main():
#
#    parser = argparse.ArgumentParser()
#    parser.add_argument("-port", type=int, help="MQTT Port", default=1883)
#    parser.add_argument("-host", help="MQTT Host IP", default="localhost")
#
#    args = parser.parse_args()
#
#    filepath = '../config/node_desc_a.json'
#
#    # Ensure that we can open the nodes file
#    node_descriptor = None
#
#    try:
#        f = open(filepath, 'r')
#        node_descriptor = json.load(f)
#        f.close()
#    except Exception as e:
#        print(repr(e))
#        print('Could not open given node file {}'.format(filepath))
#        return -1
#
#    v_node = node.Node(args.host, args.port, node_descriptor)
#    v_node.start()
#
#    print('Publishable topics: {}'.format(v_node.publishable_links))
#    print('Subscriptable topics: {}'.format(v_node.subscribable_links))
#    print('Gettable topics: {}'.format(v_node.gettable_links))
#    print('Puttable topics: {}'.format(v_node.puttable_links))
#
#    # Stop the node that we started 
#    v_node.stop()
#
#    print('Succesfully stopped the node!')
#
#
#if(__name__ == "__main__"):
#    main()
