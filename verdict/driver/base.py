

class BaseDriver(object):

    def __init__(self):
        self.id = 'basedriver'

    def row_count(self, name):
        raise NotImplementedError

    def columns(self, name):
        """
        @param name  A fully quantified name for a data source
        @return  A list of (attr name, attr type)
        """
        raise NotImplementedError

    def name_for_sample(self, sample_id):
        """Returns a fully quantified name to store a sample data
        """
        raise NotImplementedError

    def retrieve_data(self, name):
        """Returns all rows from the data source.
        @return (data, desc) where
            data = [(attr values), ... ]
            desc = [(col name, col type), ...]
        """
        raise NotImplementedError

    def execute(self, query):
        """
        @param query  A query in json string format
        @return  A result in json string format
        """
        raise NotImplementedError