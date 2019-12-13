from abc import *
from classes.Job import Job
from classes.abstract_classes.AbstractJobRunner import AbstractJobRunner
from classes.abstract_classes.AbstractContext import AbstractContext
from classes.abstract_classes.AbstractMapper import AbstractMapper
from classes.abstract_classes.AbstractReducer import AbstractReducer
import os.path as Path
from decimal import Decimal
from math import sin, cos, sqrt, atan2, radians

#-----------------------------Job1---------------------------#

class PassengerDataMapper(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            Map the passenger data into the following format
                <Departure airport code>: isDeparture|<*ticket identifier>
                <Arrival airport code>:   isArrival|<*ticket identifier>

                <*ticket identifier> = <passenger-ID>-<Row number in input data>
        """
        value_list = value.split(",")
        index = key
        passenger_id = value_list[0]
        ticket_indentifier = passenger_id + "-" + index
        dep_airport_code = value_list[2]
        arr_airport_code = value_list[3]

        #{dep_airport_code: "isDepartureOrArrival|ticket_indentifier}
        context.write(dep_airport_code, "isDeparture|" + ticket_indentifier)
        context.write(arr_airport_code, "isArrival|" + ticket_indentifier)

class AirportDataMapper(AbstractMapper):
    def map(self, key: str, value: str, context: AbstractContext):
        """
            Map the aiport data into the following format.
                <Airport code>: isCoordinate|<latitude>,<longitude>
        """
        value_list = value.rstrip("\n").split(",")
        airport_code = value_list[1]
        latitude = value_list[2]
        longitude = value_list[3]
        #{dep_airport_code: "isCoordinate|airport-latitude, airport-longitude|passenger_id|}
        context.write(airport_code, "isCoordinate|" + latitude + "," + longitude)

class Job1Reducer(AbstractReducer):
    def reduce(self, key: str, values: list, context: AbstractContext):
        """
            This reducer merge the data of a flight ticket with an airport based on an airport code.

            Reduce the data of three different formats bellow:
                <Departure airport code>: isDeparture|<*ticket identifier>
                <Arrival airport code>:   isArrival|<*ticket identifier>
                <Airport code>: isCoordinate|<latitude>,<longitude>

            Into:
                <ticket_identifier(passengerID & lineIndex)>/<DEPARTURE or ARRIVAL>: <FAA_CODE>|<latitude, longitude>

        """
        airport_code = key
        longitude = ""
        latitude = ""

        departure_airports = {} #{ticket_indentifier: Departure_airport_code}
        arrival_airports = {} #{ticket_indentifier: Arrival_airport_code]}
        for val in values:
            value_list = val.split("|")
            data_flag = value_list[0]
            if data_flag == "isCoordinate":
                latitude_and_longitude = value_list[1].split(",")
                latitude = latitude_and_longitude[0]
                longitude = latitude_and_longitude[1]
            elif data_flag == "isDeparture":
                data = val.split("|")
                ticket_identifier = data[1]
                departure_airports[ticket_identifier] = airport_code
            elif data_flag == "isArrival":
                data = val.split("|")
                ticket_identifier = data[1]
                arrival_airports[ticket_identifier] = airport_code

        is_invalid_airport = False
        if latitude == "" and longitude == "":
            is_invalid_airport = True

        if not is_invalid_airport:
            for ticket_identifier in departure_airports:
                context.write(ticket_identifier + "/DEPARTURE", departure_airports[ticket_identifier] + "|" + latitude + "," + longitude)
            for ticket_identifier in arrival_airports:
                context.write(ticket_identifier + "/ARRIVAL", arrival_airports[ticket_identifier] + "|" + latitude + "," + longitude)
#------------------------------------------ Job 1 END -----------------------------------------------------#


#------------------------------------------   Job 2   -----------------------------------------------------#
class Job2Mapper(AbstractMapper):
    """
        Map the output data from Job1 bellow:
            <ticket_identifier(passengerID | lineIndex)>/<DEPARTURE or ARRIVAL>: <FAA_CODE>|<latitude, longitude>
        Into
            ticket_identifier(passengerID | lineIndex): <isDeparture OR isArrival>|<FAA_CODE>|<latitude, longitude>'
    """
    def map(self, key: str, value: str, context: AbstractContext):
        value_list = value.rstrip("\n").split(":")
        ticket_identifier_and_flag = value_list[0].split("/")
        ticket_identifier = ticket_identifier_and_flag[0]
        departure_or_arrival_flag = ticket_identifier_and_flag[1]

        airport_code_and_coordinate = value_list[1].split("|")
        airport_code = airport_code_and_coordinate[0]
        coordinate = airport_code_and_coordinate[1]

        if departure_or_arrival_flag == "DEPARTURE":
            #print("Job2Mapper:", "isDeparture|" + airport_code + "|" + coordinate)
            context.write(ticket_identifier, "isDeparture|" + airport_code + "|" + coordinate)
        elif departure_or_arrival_flag == "ARRIVAL":
            context.write(ticket_identifier, "isArrival|" + airport_code + "|" + coordinate)
            #print("Job2Mapper:", "isArrival|" + airport_code + "|" + coordinate)


class MergeReducer(AbstractReducer):
    """
        Merge both the arrival and the departure airport coordinates of the same ticket ID

        Reduce bellow:
            ticket_identifier(passengerID | lineIndex): <isDeparture OR isArrival>|<FAA_CODE>|<latitude, longitude>'

        Into:
            <PassengerID>-<index>:<DEPARTURE_FAA_CODE>|Departure_latitude,Departure_longitude|<Arrival_FAA_CODE>|Arrival_latitude,Arrival_longitude
            eg) EDV2089LK5-7:DEN|39.861656,-104.673178|PVG|31.143378,121.805214
    """
    def reduce(self, key: str, values: list, context: AbstractContext):
        ticket_identifier = key
        departure_location = "" # Airport ID and its coordinate
        arrival_location = ""
        for flag_and_airport_info in values:
            flag_airportcode_coordinate = flag_and_airport_info.split("|")
            data_flag = flag_airportcode_coordinate[0]
            airport_code = flag_airportcode_coordinate[1]
            coordinate = flag_airportcode_coordinate[2]

            if data_flag == "isDeparture":
                departure_location = airport_code + "|" + coordinate
            elif data_flag == "isArrival":
                arrival_location = airport_code + "|" + coordinate
        if departure_location != "" and arrival_location != "":
            context.write(ticket_identifier, departure_location + "|" + arrival_location)

#------------------------------------------- Job 3 -------------------------------------------------#

class Job3Mapper(AbstractMapper):
    """
        Map the data in the format bellow:
            <PassengerID>-<index>:<DEPARTURE_FAA_CODE>|Departure_latitude,Departure_longitude|<Arrival_FAA_CODE>|Arrival_latitude,Arrival_longitude
            eg) 'EDV2089LK5-7:DEN|39.861656,-104.673178|PVG|31.143378,121.805214'
            * Each line represents a "ticket"(One of the flights a passenger took)

        To the key & val pairs bellow:
            <passenegrID> : <distance_traveled_by_the_flight>
    """
    def map(self, key: str, value: str, context: AbstractContext):
        value_list = value.rstrip("\n").split(":")
        passenger_id = value_list[0].split("-")[0]

        location_list = value_list[1].split("|")

        departure_coord = location_list[1].split(",")
        departure_latitude = Decimal(departure_coord[0])
        departure_longitude = Decimal(departure_coord[1])

        arrival_coord = location_list[3].split(",")
        arrival_latitude = Decimal(arrival_coord[0])
        arrival_longitude = Decimal(arrival_coord[1])
        distance_travelled = self.get_distance(departure_latitude, departure_longitude, arrival_latitude, arrival_longitude)
        context.write(passenger_id, str(distance_travelled))

    def get_distance(self, departure_lat, departure_lon, arrival_lat, arrival_lon):
        # approximate radius of earth in km
        R = 6373.0

        lat1 = radians(departure_lat)
        lon1 = radians(departure_lon)
        lat2 = radians(arrival_lat)
        lon2 = radians(arrival_lon)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c
        return distance


class Job3Reducer(AbstractReducer):
    """
        This reducer adds all the distance each passenger travelled.
        Reduce the format bellow:
            <passenegrID> : [<distance_traveled_by_the_flight>, <distance_traveled_by_the_flight>...]

        To bellow:
            <passenegrID> : <TOTAL_distance_traveled_by_the_flight>
        Also sort the total distance
    """
    def reduce(self, key: str, values: list, context: AbstractContext):
        distance_travelled = Decimal("0")
        for distance_travelled_str in values:
            distance_travelled += Decimal(distance_travelled_str)
        distance_travelled = round(distance_travelled, 3)
        context.write(key, str(distance_travelled) + "km")

#------------------------------------------- Job 4 -------------------------------------------------#
# This job simply finds the passenger with the most distance travelled.
class Job4Mapper(AbstractMapper):
    """
        Map the data in the format bellow:
            <PassengerID>:<Distance_travelled>km

        To the key & val pairs bellow:
            "SAME_KEY_FOR_ALL" : <PassengerID>,<Distance_travelled>
    """
    def map(self, key: str, value: str, context: AbstractContext):
        context.write("SAME_KEY_FOR_ALL", value.rstrip("\nkm"))


class Job4Reducer(AbstractReducer):
    """
        Goal: Get the passenger with the highest earned mileage and the mileage itself.

        Map the data in the format bellow:
            "SAME_KEY_FOR_ALL" : <PassengerID>:<Distance_travelled>


        To the key & val pair bellow:
            <passenegrID> : <distance_traveled_by_the_flight>
    """
    def reduce(self, key: str, values: list, context: AbstractContext):
        max_distance = []
        max_distance.append(Decimal("0"))
        passenger_with_the_most_travelled_distance = []
        for val in values:
            passenger_and_distance = val.split(":")
            distance = Decimal(passenger_and_distance[1])

            if distance > max_distance[0]:
                max_distance.clear()
                passenger_with_the_most_travelled_distance.clear()

                max_distance.append(distance)
                passenger_id = passenger_and_distance[0]
                passenger_with_the_most_travelled_distance.append(passenger_id)

            elif distance == max_distance[0]:
                max_distance.append(distance)
                passenger_id = passenger_and_distance[0]
                passenger_with_the_most_travelled_distance.append(passenger_id)

        for i in range(len(passenger_with_the_most_travelled_distance)):
            passenger_id = passenger_with_the_most_travelled_distance[i]
            distance = max_distance[i]
            context.write(passenger_id, str(distance) + "km")


class JobRunner(AbstractJobRunner):
    def run(self, commandArgs: list, tempoFileBufferPath: Path):
        job1 = Job()
        job1.set_mapper(PassengerDataMapper, commandArgs[0])
        job1.set_mapper(AirportDataMapper, commandArgs[1])
        job1.set_reducer(Job1Reducer, tempoFileBufferPath)
        self.add_to_job_queue(job1)

        job2 = Job()
        job2.set_mapper(Job2Mapper, tempoFileBufferPath)
        job2.set_reducer(MergeReducer, tempoFileBufferPath)
        self.add_to_job_queue(job2)

        job3 = Job()
        job3.set_mapper(Job3Mapper, tempoFileBufferPath)
        job3.set_reducer(Job3Reducer, "(4)DistanceTravelledByEachPassenger.txt")
        job3.set_reducer(Job3Reducer, tempoFileBufferPath)
        self.add_to_job_queue(job3)

        job4 = Job()
        job4.set_mapper(Job4Mapper, tempoFileBufferPath)
        job4.set_reducer(Job4Reducer, "(4)HighestAirmile.txt")
        self.add_to_job_queue(job4)
