from pyspark import SparkContext
from optparse import OptionParser
from fileUtil import FileUtil
import codecs
import json

def processkarmaconfig(jsonkc):
    classes = []

    for config in jsonkc:
        if "roots" in config:
            for root in config["roots"]:
                if "root" in root:
                    classes.append(getclassnamefromroot(root["root"]))

    print "Returning classes:", classes
    return classes

def getclassnamefromroot(root):
    pieces = root.split("/")
    classinstance = pieces[len(pieces)-1]
    classname = classinstance[0:len(classinstance)-1]
    return classname

if __name__ == "__main__":

    sc = SparkContext(appName="DIG-FILTER-HT")

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
    karmaconfigpath = args[4]

    fileUtil = FileUtil(sc)
    kc = sc.textFile(karmaconfigpath).first()
    

    jsonkc = json.loads(kc)

    classes = processkarmaconfig(jsonkc)

    input_rdd = fileUtil.load_json_file(inputFilename, inputFileFormat, c_options)

    
    for classe in classes:
        print "Save class:", classe
        rdd_out = input_rdd.filter(lambda x: x[1]["a"] == classe).coalesce(21)
        if not rdd_out.isEmpty():
            fileUtil.save_json_file(rdd_out, outputPath + "/"+ classe, outputFileFormat, c_options)