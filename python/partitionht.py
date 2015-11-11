#!/usr/bin/env python
import json

class PartitionHt:

    def __init__(self):
        pass

    def filter_docs(self, classname,doc):
        if "a" in doc:
            if doc["a"] == classname:
                return json.dumps(doc)











