import batch
import csv

class SimpleETL(batch.ETLStep):
    '''This class:

        Extracts data from a CSV file
        Transforms the data by reversing the URL string
        Loads the transformed data into another CSV file

    '''

    URL_COLUMN = 5
    def extract(self, source):
        '''
        Load the CSV specified by the source variable (which stores the path
        to the file)

        '''
        result = []
        with open(source, 'rb') as f:
            reader = csv.reader(f)

            for l in reader:
                result.append(l)        # using yield is better/faster

        return result

    def transform(self, data):
        '''
        Transform the URL by reversing it.

        '''
        for row in data:
            url = row[self.URL_COLUMN]
            reversed_url = url[::-1]
            row[self.URL_COLUMN] = reversed_url

        return data

    def load(self, data, sink):
        '''
        Write data to file specified by sink.

        '''
        with open(sink, 'wb') as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row)

if __name__ == '__main__':
    simple_etl = SimpleETL()
    extracted_data = simple_etl.extract('data/flat_data.csv')
    transformed_data = simple_etl.transform(extracted_data)
    simple_etl.load(transformed_data, 'data/reversed_url.csv')
