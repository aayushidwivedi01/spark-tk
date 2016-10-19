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

""" Tests Linear Regression scoring engine """
import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class CollaborativeFiltering(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(CollaborativeFiltering, self).setUp()
        dataset = self.get_file("collab_filtering.csv")
        schema = [("user", str), ("product", str), ("rating", float)]

        self.frame = self.context.frame.import_csv(dataset, schema=schema)
        self.frame.add_columns(
            lambda x: [x["user"][5:], x['product'][5:]],
            [("user_int", int), ("item_int", int)])

    def test_model_publish(self):
        """Test publishing a linear regression model"""
        model = self.context.models.collaborativefiltering \
            .collaborative_filtering \
            .train(self.frame, "user_int", "item_int", "rating", max_steps=15)
        predict = model.predict(
            self.frame, "user_int", "item_int")
        test_rows = predict.to_pandas(predict.count())

        file_name = self.get_name("CollaborativeFiltering")
        model_path = model.export_to_mar(self.get_export_file(file_name))
        with scoring_utils.scorer(model_path) as scorer:
            for _, i in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["user_int", "item_int", "rating"], list(i[0:3])))])
                print res.json()
                self.assertEqual(
                    i["predicted_value"], res.json()["data"][0]['Prediction'])

            


if __name__ == '__main__':
    unittest.main()
