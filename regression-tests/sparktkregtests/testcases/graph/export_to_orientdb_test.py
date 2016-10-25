# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Tests the weighted_degree on a graph"""

import unittest
import uuid
from sparktkregtests.lib import sparktk_test, config

class ExportToOrientDBTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        """Build frames and graphs to be tested"""
        super(ExportToOrientDBTest, self).setUp()
        graph_data = self.get_file("clique_10_new.csv")
        schema = [('src', str),
                  ('dst', str),
                  ('src_type', str),
                  ('dst_type', str),
                  ('edge_type', str)]
        
        # set up the vertex frame, which is the union of the src and
        # the dst columns of the edges
        self.frame = self.context.frame.import_csv(graph_data, schema=schema)
        self.vertices = self.frame.copy()
        self.vertices2 = self.frame.copy()
        self.vertices.rename_columns({"src": "id", "src_type":"gender"})
        self.vertices.drop_columns(["dst", "dst_type", "edge_type"])
        self.vertices2.rename_columns({"dst": "id", "dst_type":"gender"})
        self.vertices2.drop_columns(["src", "src_type", "edge_type"])
        self.vertices.append(self.vertices2)
        self.vertices.drop_duplicates(["id"])
        self.frame.drop_columns(["src_type", "dst_type"])

        self.sparktk_graph = self.context.graph.create(self.vertices, self.frame)

    def test_default_export(self):
        """Tests output of export with default parameters"""
        url = self.get_url()
        export_result = self.sparktk_graph.export_to_orientdb(
            db_url=url, user_name="admin", password="admin",
            root_password="orient123")

        #verify export_result against known values
        self.assertEqual(url, export_result.db_uri)
        self.assertItemsEqual(export_result.edge_types.keys(),['E'])
        self.assertItemsEqual(export_result.edge_types.values(), [165])
        self.assertItemsEqual(export_result.vertex_types.keys(),['V'])
        self.assertItemsEqual(export_result.vertex_types.values(), [54]) 
        self.assertEqual(
            export_result.exported_edges_summary["Total Exported Edges Count"],
            self.sparktk_graph.graphframe.edges.count())
        self.assertEqual(
            export_result.exported_vertices_summary["Total Exported Vertices Count"],
            self.sparktk_graph.graphframe.vertices.count())

    def test_export_edge_vertex_type(self):
        """Tests export with edge and vertex type parameters"""
        url = self.get_url()
        export_result = self.sparktk_graph.export_to_orientdb(
            db_url=url, user_name="admin", password="admin",
            root_password="orient123", vertex_type_column_name="gender",
            edge_type_column_name="edge_type")

        #verify export_result against known values
        self.assertEqual(url, export_result.db_uri)
        self.assertItemsEqual(export_result.edge_types.keys(),['follower', 'friend'])
        self.assertItemsEqual(export_result.edge_types.values(), [85, 80])
        self.assertItemsEqual(export_result.vertex_types.keys(),['F', 'M'])
        self.assertItemsEqual(export_result.vertex_types.values(), [26, 28])

        self.assertEqual(
            export_result.exported_edges_summary["Total Exported Edges Count"],
            self.sparktk_graph.graphframe.edges.count())
        self.assertEqual(
            export_result.exported_vertices_summary["Total Exported Vertices Count"],
            self.sparktk_graph.graphframe.vertices.count())

    def test_bad_db_url(self):
        """Tests bad orientDB url throws exception"""
        with self.assertRaisesRegexp(
                Exception, "Error on opening database \'bad\'"):
            export_result = self.sparktk_graph.export_to_orientdb(
                db_url="bad", user_name="admin", password="admin",
                root_password="orient123", vertex_type_column_name="gender",
                edge_type_column_name="edge_type")

    def test_bad_root_passwd(self):
        """Tests wrong orientDB password throws exception"""
        url = self.get_url()
        with self.assertRaisesRegexp(
                Exception, "Wrong user/password"):
            export_result = self.sparktk_graph.export_to_orientdb(
                db_url=url, user_name="admin", password="admin",
                root_password="bad", vertex_type_column_name="gender",
                edge_type_column_name="edge_type")

    def test_bad_vertex_type_column_name(self):
        """Tests incorrext vertex type column name throws exception"""
        url = self.get_url()
        with self.assertRaisesRegexp(
                Exception, "Cannot connect to the remote server/database"):
            export_result = self.sparktk_graph.export_to_orientdb(
                db_url=url, user_name="admin", password="admin",
                root_password="bad", vertex_type_column_name="ERR",
                edge_type_column_name="edge_type")

    def get_url(self):
        hostname = config.hostname
        port = 2424
        db = "db_" + str(uuid.uuid1().hex)
        return "remote:" + str(hostname) + ":" + str(port) + "/" + db

if __name__ == "__main__":
    unittest.main()
