from mrjob.job import MRJob
import csv

class MRAvgWords(MRJob):
    def mapper(self, _, line):
        parsed = csv.reader(line.splitlines())
        for row in parsed:
            if row[7] != '0' and row[0] != 'business_id':
                values = row[3]
                yield 'Sum', values
    def reducer(self, key, values):
        totalSum = 0
        Count = 0
        for value in values:
            totalSum += int(value)
            Count += 1
        yield 'Average rating of places marked cool: ', totalSum/Count

if __name__ == '__main__':
    def main():
         MRAvgWords.run()    
    main()