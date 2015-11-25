from pyspark import SparkContext
from optparse import OptionParser
from fileUtil import FileUtil
import codecs
import json
import re

south_california_backpage_regex = ["^http[s]?://[^/]*bakersfield.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*imperial.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*inlandempire.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*longbeach.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*losangeles.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*palmsprings.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*orangecounty.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*palmdale.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*sandiego.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*sanfernandovalley.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*sanluisobispo.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*santabarbara.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*santamaria.backpage.com/FemaleEscorts\.*","^http[s]?://[^/]*ventura.backpage.com/FemaleEscorts\.*"]

def filterdocs(key):
	for pattern in south_california_backpage_regex:
		if re.match(pattern,key.strip()):
			return True
		
	return False

if __name__ == "__main__":

    sc = SparkContext(appName="DIG_FILTER_SOUTH-CALIFORNIA")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat inputPath " \
            "outputFilename outoutFileFormat karmaconfigpath"
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

    rdd_out = input_rdd.filter(lambda x : filterdocs(x[0])).coalesce(21)
    if not rdd_out.isEmpty():
    	fileUtil.save_json_file(rdd_out, outputPath, outputFileFormat, c_options)