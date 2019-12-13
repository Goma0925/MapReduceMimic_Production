from abc import *
from classes.Job import Job
from classes.abstract_classes.AbstractJobRunner import AbstractJobRunner
from classes.abstract_classes.AbstractContext import AbstractContext
from classes.abstract_classes.AbstractMapper import AbstractMapper
from classes.abstract_classes.AbstractReducer import AbstractReducer
import os.path as Path


class PassengerMapper(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        flight_id = value.split(",")[1]
        passenger_id = value.split(",")[0]
        context.write(flight_id, passenger_id)


class Reducer(AbstractReducer):
    def reduce(self, key: str, values: list, context: AbstractContext):
        total_count = 0
        flight_id = key
        for passenger_id in values:
            total_count += 1
        context.write(flight_id, total_count)


class JobRunner(AbstractJobRunner):
    def run(self, commandArgs: list, tempoFileBufferPath: Path):
        job = Job()
        job.set_mapper(PassengerMapper, commandArgs[0])
        job.set_reducer(Reducer, "(3)Number_of_passengers_on_each_flight.txt")
        self.add_to_job_queue(job)

