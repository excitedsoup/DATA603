from mrjob.job import MRJob
import csv

class MRAvgWords(MRJob):
    def mapper(self, _, line):
        parsed = csv.reader(line.splitlines())
        for row in parsed:
            if row[5] == 'review':
                key = row[1][:7]
                values = 1
                yield key, values
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    def main():
         MRAvgWords.run()    
    main()