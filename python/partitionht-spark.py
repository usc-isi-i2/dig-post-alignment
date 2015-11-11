#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
from fileUtil import FileUtil
#from partitionht import PartitionHt
import json

if __name__ == "__main__":

    AdultService = "AdultService"
    PhoneNumber = "PhoneNumber"
    Offer = "Offer"
    PersonOrOrganization = "PersonOrOrganization"
    WebPage = "WebPage"
    EmailAddress = "EmailAddress"

    sc = SparkContext(appName="DIG-FILTER-HT")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat inputPath " \
            "outputFilename outoutFileFormat"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    inputFileFormat = args[1]
    outputPath = args[2]
    outputFileFormat = args[3]

    fileUtil = FileUtil(sc)
    input_rdd = fileUtil.load_json_file(inputFilename, inputFileFormat, c_options)
    #partition = PartitionHt()

    #as_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(AdultService, x)).filter(lambda x: x[1] is not None).coalesce(42)
    as_rdd = input_rdd.filter(lambda x: x[1]["a"] == AdultService).coalesce(21)
    #pn_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(PhoneNumber, x)).filter(lambda x: x[1] is not None).coalesce(42)
    pn_rdd = input_rdd.filter(lambda x: x[1]["a"] == PhoneNumber).coalesce(21)
    #o_rdd =  input_rdd.mapValues(lambda x: partition.filter_docs(Offer, x)).filter(lambda x: x[1] is not None).coalesce(42)
    o_rdd =  input_rdd.filter(lambda x: x[1]["a"] == Offer).coalesce(21)
    #po_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(PersonOrOrganization, x)).filter(lambda x: x[1] is not None).coalesce(42)
    po_rdd = input_rdd.filter(lambda x: x[1]["a"] == PersonOrOrganization).coalesce(21)
    #wp_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(WebPage, x)).filter(lambda x: x[1] is not None).coalesce(42)
    wp_rdd = input_rdd.filter(lambda x: x[1]["a"] == WebPage).coalesce(21)
    #ea_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(EmailAddress, x)).filter(lambda x: x[1] is not None).coalesce(42)
    ea_rdd = input_rdd.filter(lambda x: x[1]["a"] == EmailAddress).coalesce(21)

    fileUtil.save_json_file(as_rdd, outputPath + "/"+ AdultService, outputFileFormat, c_options)
    fileUtil.save_json_file(pn_rdd, outputPath + "/" + PhoneNumber, outputFileFormat, c_options)
    fileUtil.save_json_file(o_rdd, outputPath + "/" + Offer, outputFileFormat, c_options)
    fileUtil.save_json_file(po_rdd, outputPath + "/" + PersonOrOrganization, outputFileFormat, c_options)
    fileUtil.save_json_file(wp_rdd, outputPath + "/" + WebPage, outputFileFormat, c_options)
    fileUtil.save_json_file(ea_rdd, outputPath + "/" + EmailAddress, outputFileFormat, c_options)

