#!/usr/bin/env python
import json

class FilterAdultservices:

    def __init__(self):
        pass

    def map_docs(self,doc):
		
		jImage = doc["image"]
		if isinstance(jImage,dict):
			jImageArray=[]
			jImageArray.append(jImage)
		if isinstance(jImage,list):
			jImageArray = jImage
		for obj in jImageArray:
			if "isSimilarTo" in obj:
				jSimilar = obj["isSimilarTo"]
				if isinstance(jSimilar,list):
					if len(obj["isSimilarTo"]) > 500:
						obj["isSimilarTo"] = []

		return doc
         
