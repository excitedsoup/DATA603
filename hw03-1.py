from mrjob.job import MRJob
import sys
import csv
inFile = str(sys.argv[1])

class MRAvgWords(MRJob):
    def mapper(self, _, line):
        with open(inFile, newline='') as csvfile:    
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if(row[5] == 'review'):
                    yield row[2], row[4]
    # def reducer(self, key, values):
    #     yield key, values

if __name__ == '__main__':
    def main():
         MRAvgWords.run()       
    main()