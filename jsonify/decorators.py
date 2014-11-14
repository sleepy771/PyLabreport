__author__ = 'filip'

import json


def jsonify(**clazz_dict):
    def decorator(clazz):
        def read_json(json_obj_str):
            json.loads(json_obj_str, object_hook=clazz.__init__)


# @jsonify(['param_a', 'param_b', 'param_c'])
# class A(object):
#     def __init__(self, *args, **kwargs):
#         self.param_a = ...
#         self.param_b = ...
#         self.param_c = ...
#         self.param_d = ...