import time
import json
import vizier.node as node
import vizier.vizier as vizier
import unittest

class TestVizierNodes(unittest.TestCase):
    
    def setUp(self):
        path_a = '../config/node_desc_a.json'
        path_b = '../config/node_desc_b.json'

        try:
            f = open(path_a, 'r')
            node_descriptor_a = json.load(f)
            f.close()
        except Exception as e:
            print(repr(e))
            print('Could not open given node file {}'.format(path_a))
            return -1
    
        try:
            f = open(path_b, 'r')
            node_descriptor_b = json.load(f)
            f.close()
        except Exception as e:
            print(repr(e))
            print('Could not open given node file {}'.format(path_b))
            return -1
    
        self.node_a = node.Node('localhost', 1883, node_descriptor_a)
        self.node_a.start()
    
        self.node_b = node.Node('localhost', 1883, node_descriptor_b)
        self.node_b.start()

        self.vizier = vizier.Vizier('localhost', 1883, ['b'])
        self.vizier.start()
   
    def test_publishable_links(self):
        print(self.vizier.get_links())
        print(self.vizier.get_deps())
        print(self.vizier.get_link_deps())
        print(self.vizier.verify_deps())

    def tearDown(self):
        self.node_a.stop()
        self.node_b.stop()
        self.vizier.stop()
