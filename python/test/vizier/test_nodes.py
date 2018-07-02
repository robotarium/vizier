import json
import vizier.node as node
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
   
    def test_publishable_links(self):
        self.assertEqual(self.node_a.publishable_links, {'a/a_sub'})
        self.assertEqual(self.node_b.publishable_links, set())

    def test_subscribable_links(self):
        self.assertEqual(self.node_a.subscribable_links, set())
        self.assertEqual(self.node_b.subscribable_links, {'a/a_sub'})

    def test_gettable_links(self):
        self.assertEqual(self.node_a.gettable_links, set())
        self.assertEqual(self.node_b.gettable_links, set())

    def test_puttable_links(self):
        self.assertEqual(self.node_a.puttable_links, set())
        self.assertEqual(self.node_b.puttable_links, {'b/b_sub/b_sub/b_sub'})

    def tearDown(self):
        self.node_a.stop()
        self.node_b.stop()
