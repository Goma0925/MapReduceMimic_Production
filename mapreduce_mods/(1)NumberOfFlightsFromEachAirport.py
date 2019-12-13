from abc import *
from classes.Job import Job
from classes.abstract_classes.AbstractJobRunner import AbstractJobRunner
from classes.abstract_classes.AbstractContext import AbstractContext
from classes.abstract_classes.AbstractMapper import AbstractMapper
from classes.abstract_classes.AbstractReducer import AbstractReducer
import os.path as Path

class PassengerDataMapper(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            Map the passenger data into the following format
                <Departure airport code>: "isPassengerData"|<FlightID>
        """
        value_list = value.split(",")
        departure_airport_code = value_list[2]
        flight_id = value_list[1]

        context.write(departure_airport_code, "isPassengerData|" + flight_id)


class AirportDataMapper(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            Map the airport data into the following format
                <Departure airport code>: isAirportData|<Name of the airport *Always blank>
        """
        value_list = value.split(",")
        departure_airport_code = value_list[1]
        airport_name = value_list[0]

        context.write(departure_airport_code, "isAirportData|" + airport_name)


class Reducer1(AbstractReducer):
    """
        This reducer merge the name of the departure airport and the airport code,
        and count how many flights departed from the airport.

        Reduce the data in the following format:
            <Departure airport code>: <isPassengerData OR isAirportData>|<FlightID>
        Into:
            <Departure airport code>-<Airport name>: A number of flights from the departure airport.

    """
    def reduce(self, key: str, values: list, context: AbstractContext):
        flight_ids = []
        departure_airport_code = key
        airport_name = ""

        #Retrieve all the flight ID and its departure airport name from passenger data.
        for val in values:
            value_list = val.split("|")
            if value_list[0] == "isPassengerData":
                # Collect the flight ID from the passenger data.
                flight_id = value_list[1]
                if flight_id not in flight_ids:
                    flight_ids.append(flight_id)
            elif value_list[0] == "isAirportData":
                # Collect the airport name from the airport data.
                airport_name = value_list[1]

        if airport_name == "":
            airport_name = "UNKNOWN AIRPORT CODE"
        context.write(airport_name + "(" + departure_airport_code + ")", str(len(flight_ids)))



class JobRunner(AbstractJobRunner):
    def run(self, commandArgs: list, tempoFileBufferPath: Path):
        job1 = Job()
        job1.set_mapper(PassengerDataMapper, commandArgs[0])
        job1.set_mapper(AirportDataMapper, commandArgs[1])
        job1.set_reducer(Reducer1, "(1)Number_of_flights_from_each_airport.txt")
        self.add_to_job_queue(job1)

