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

""" Tests graph import from orientdb"""

import unittest
import uuid
from sparktkregtests.lib import sparktk_test, config

def get_url():
        hostname = config.hostname
        port = 2424
        db = "db_" + str(uuid.uuid1().hex)
        return "remote:" + str(hostname) + ":" + str(port) + "/" + db

class ImportOrientDBGraphTest(sparktk_test.SparkTKTestCase):
    URL = get_url()

    def setUp(self):
        """Build frames and graphs to be tested"""
        super(ImportOrientDBGraphTest, self).setUpClass()
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

        self.graph = self.context.graph.create(self.vertices, self.frame)

    def test_import_edge_vertex_type(self):
        """Tests export with edge and vertex type parameters"""
        url = ImportOrientDBGraphTest.URL
        export_result = self.graph.export_to_orientdb(
            db_url=url, user_name="admin", password="admin",
            root_password="orient123", vertex_type_column_name="gender",
            edge_type_column_name="edge_type")

        imported_graph = self.context.graph.import_orientdb_graph(
            db_url=url, user_name="admin", password="admin",
            root_password="orient123")
        self._validate_result(imported_graph)

    def test_bad_db_url(self):
        """Tests bad orientDB url throws exception"""
        with self.assertRaisesRegexp(
                Exception, "Error on opening database \'bad\'"):
            imported_graph = self.context.graph.import_orientdb_graph(
                db_url="bad", user_name="admin", password="admin",
                root_password="orient123")

    def test_bad_root_passwd(self):
        """Tests wrong orientDB password throws exception"""
        url = ImportOrientDBGraphTest.URL
        with self.assertRaisesRegexp(
                Exception, "Wrong user/password"):
            imported_graph = self.context.graph.import_orientdb_graph(
                db_url=url, user_name="admin", password="admin",
                root_password="bad")

    def test_bad_user_name(self):
        """Tests wrong db username throws exception"""
        url = ImportOrientDBGraphTest.URL
        with self.assertRaisesRegexp(
                Exception, "Unable to open database"):
            imported_graph = self.context.graph.import_orientdb_graph(
                db_url=url, user_name="ERR", password="admin",
                root_password="orient123")
   
    def test_bad_user_password(self):
        """Tests wrong user password throws exception"""
        url = ImportOrientDBGraphTest.URL
        with self.assertRaisesRegexp(
                Exception, "Unable to open database"):
            imported_graph = self.context.graph.import_orientdb_graph(
                db_url=url, user_name="admin", password="ERR",
                root_password="orient123")

    def _validate_result(self, imported_graph):

        #validate vertices
        actual_vertices_frame = imported_graph.create_vertices_frame()
        actual_vertices_pf = actual_vertices_frame.to_pandas(self.vertices.count())
        expected_vertices_pf = self.vertices.to_pandas(self.vertices.count())
        actual_vertex_ids = actual_vertices_pf["id"].tolist()
        expected_vertex_ids = expected_vertices_pf["id"].tolist()
        self.assertItemsEqual(actual_vertex_ids, expected_vertex_ids)

        #validate edges
        edges_frame = imported_graph.create_edges_frame()
        actual_edges = edges_frame.to_pandas(
            edges_frame.count()).values.tolist()
        expected_edges = self.frame.to_pandas(
            self.frame.count()).values.tolist()

        #switch dst and src columns to match the new frame schema
        expected_edges = [[dst, src, edge_type] for src, dst, edge_type in expected_edges]
        actual_edges.sort(key=lambda x:x[0])
        expected_edges.sort(key=lambda x:x[0])

        self.assertItemsEqual(
            actual_edges,
            expected_edges)

        #validate degrees of imported graph against that of the original graph
        actual_degrees_frame = imported_graph.degrees()
        expected_degrees_frame = self.graph.degrees()

        self.assertFramesEqual(actual_degrees_frame, expected_degrees_frame)

if __name__ == "__main__":
    unittest.main()
