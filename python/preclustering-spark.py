#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
from fileUtil import FileUtil
from partitionht import PartitionHt

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
    partition = PartitionHt()

    as_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(AdultService,x)).filter(lambda  x: x[1] is not None)
    pn_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(PhoneNumber,x)).filter(lambda x: x[1] is not None)
    o_rdd =  input_rdd.mapValues(lambda x: partition.filter_docs(Offer,x)).filter(lambda x: x[1] is not None)
    po_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(PersonOrOrganization,x)).filter(lambda x: x[1] is not None)
    wp_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(WebPage,x)).filter(lambda x: x[1] is not None)
    ea_rdd = input_rdd.mapValues(lambda x: partition.filter_docs(EmailAddress,x)).filter(lambda x: x[1] is not None)

    fileUtil.save_text_file(as_rdd, outputPath + "/adultservice", outputFileFormat, c_options)
    fileUtil.save_text_file(pn_rdd, outputPath + "/phonenumber", outputFileFormat, c_options)
    fileUtil.save_text_file(o_rdd, outputPath + "/offer", outputFileFormat, c_options)
    fileUtil.save_text_file(po_rdd, outputPath + "/personororganization", outputFileFormat, c_options)
    fileUtil.save_text_file(wp_rdd, outputPath + "/webpage", outputFileFormat, c_options)
    fileUtil.save_text_file(ea_rdd, outputPath + "/emailaddress", outputFileFormat, c_options)

