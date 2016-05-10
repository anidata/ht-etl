class ETLStep(object):
    '''Template class for creating ETL steps.

    See simple.py for an example
    '''
    def extract(self, source):
        '''
        This method will extract the data from a source.

        '''
        raise NotImplementedError # Don't worry about this line

    def transform(self, data):
        '''
        Transform the data provided into something different. This is one of
        the most important parts of ETL.

        '''
        raise NotImplementedError # Don't worry about this line

    def load(self, data, sink):
        '''
        Load the data into something (e.g. file, variable, etc.)

        '''
        raise NotImplementedError # Don't worry about this line
