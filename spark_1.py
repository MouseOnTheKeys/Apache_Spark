"""The textFile() method is used to create RDDs from files (compressed
or uncompressed), directories, or glob patterns (file patterns with wildcards).
The name argument specifies the path or glob to be referenced, including
the filesystem scheme.
"""
licensefiles = sc.textFile("file:///opt/spark/licenses/")
# licensefiles = sc.textFile("file:///opt/spark/licenses/*.txt")
licensefiles
# Execute the getNumPartitions() method to return the
# number of partitions contained in the licensefiles RDD:
licensefiles.getNumPartitions()
# Count the total number of lines in all of the files combined using
# the following command:
licensefiles.count()
# Now use the wholeTextFiles() method to load the same files
# into an RDD called licensefile_pairs using the following
# command:
licensefile_pairs = sc.wholeTextFiles("file:///opt/spark/licenses/")
licensefile_pairs

licensefile_pairs.getNumPartitions()
# Use the keys() method to create a new RDD named
# filenames containing only a list of the keys from the
# licensefile_pairs RDD.
filenames = licensefile_pairs.keys()
filenames.collect()

# Use the values() method to create a new RDD named filedata
# containing an array of records containing the entire contents of each
# text file, and then return one record using the following commands:
filedata = licensefile_pairs.values()
filedata.take(1)
