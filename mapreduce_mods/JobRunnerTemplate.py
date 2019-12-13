from classes.Job import Job
from classes.abstract_classes.AbstractJobRunner import AbstractJobRunner
from classes.abstract_classes.AbstractContext import AbstractContext
from classes.abstract_classes.AbstractMapper import AbstractMapper
from classes.abstract_classes.AbstractReducer import AbstractReducer
import os.path as Path


class Mapper1(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            A user implements a mapper function here.
        """
        pass

class Mapper2(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            A user implements a mapper function here.
        """
        pass

class Mapper3(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            A user implements a mapper function here.
        """
        pass

class Reducer1(AbstractReducer):
    def reduce(self, key: str, values: list, context: AbstractContext):
        pass

class Reducer2(AbstractReducer):
    def reduce(self, key: str, values: list, context: AbstractContext):
        pass

class JobRunner(AbstractJobRunner):
    def run(self, commandArgs: list, tempoFileBufferPath: Path):
        """
            A user implements MapReduce job here.
            An example is provided below.

            commandArgs is a list of arguments a user provides in the command line interface.
            tempoFileBufferPath specifies the location of temporaty buffer that can be used for a temporary file storage
            for chained jobs.
        """
        job = Job()
        job.set_mapper(Mapper1, commandArgs[0])
        job.set_mapper(Mapper2, commandArgs[1])
        job.set_reducer(Reducer1, "outputfile1")
        job.set_reducer(Reducer1, tempoFileBufferPath)
        self.add_to_job_queue(job)

        job2 = Job()
        job2.set_mapper(Mapper3, tempoFileBufferPath)
        job2.set_reducer(Reducer1, "outputfile2")
        self.add_to_job_queue(job2)

