from mrjob.job import MRJob
import csv

class MRAvgWords(MRJob):
    def mapper(self, _, line):
        parsed = csv.reader(line.splitlines())
        for row in parsed:
            if row[5] == 'review':
                values = len(row[4].split())
                yield 'Sum', values
    def reducer(self, key, values):
        totalSum = 0
        Count = 0
        for value in values:
            totalSum += value
            Count += 1
        yield 'Average: ', totalSum/Count

if __name__ == '__main__':
    def main():
         MRAvgWords.run()    
    main()