import csv
'''
    The flatArchiver Archiver implements a csv based flat file local archive
'''

class Archiver(object):

    def __init__(self, pathToArchive):

        self.pathToArchive = pathToArchive

    def archive(self, record):

        with open(self.pathToArchive, "a", newline='') as arch:
            writer = csv.writer(arch, delimiter='\t')
            writer.writerow(record)

        return
