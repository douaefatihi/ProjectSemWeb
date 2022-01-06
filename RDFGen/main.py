from rdflib import Namespace
#from SPARQLWrapper import SPARQLWrapper, CSV
import pandas as pd
from rdflib import Graph, Literal
from rdflib import Literal
import math
#from rdflib.plugins.stores import sparqlstore
from rdflib.namespace import SOSA, SSN, XSD, RDF
from datetime import datetime
import copy
from Meteo import Meteo



class Main():
    def __init__(self):
        self.csv = None
        self.orderedAndFiltered = None
        self.rdfGraph = Graph()
        self.currentSensor = None
        self.currentRoom = None

        # Deffinition of NameSpaces
        self.ROOM = Namespace("https://territoire.emse.fr/kg/emse/fayol/4ET/")
        self.SENSOR = Namespace("http://localhost:3030/sensor/")
        self.OBSERVATION = Namespace("http://localhost:3030/observation/")
        self.CORE = Namespace("https://w3id.org/rec/core/")
        self.SEAS = Namespace("https://w3id.org/seas/")

    def csv_to_pd(self):
        data = pd.read_csv("data/data.csv")
        return data



    #Create our RDF Graph and Adding the sensor's observations to our RDF Graph
    def sensorObserv(self, row, idRow):
        """Connect current sensor to its observation"""
        timestmp = int(row["time"])/10**9
        date = datetime.fromtimestamp(timestmp)
        date = date.strftime("%Y-%m-%dT%H:%M:%S")
        #------------Connect each observations to the sensor which were observed by -------------------------
        # Remark: some sensors didn t capture an observation(temp, humid, limus) so we are not going to take them in consideration even if we get any result.
        
        #*****For Temperature******

        if row["TEMP"] and  math.isnan(row["TEMP"])==False:
            #print('First Heeeeeeeeeeeere')
            #print(row["TEMP"])
            #print(type(row["TEMP"]))
            temp = self.OBSERVATION[str(idRow)+'is'+str(self.currentSensor)]
            self.rdfGraph.add((temp, RDF.type, SOSA.Observation))
            self.rdfGraph.add((temp, SOSA.isObservedBy, self.SENSOR[self.currentSensor]))       
            self.rdfGraph.add((temp, SOSA.hasSimpleResult, Literal(str(float(row["TEMP"])), datatype=XSD.float)))
            self.rdfGraph.add((temp, SOSA.ObservableProperty,self.ROOM["{}#temperature".format(self.currentRoom)]))
            self.rdfGraph.add((temp, SOSA.resultTime,Literal(date, datatype=XSD.dateTime)))

        #*****For Humidity******
        if row["HMDT"] and math.isnan(row["HMDT"])==False:
            humidity = self.OBSERVATION[str(idRow)+'is'+str(self.currentSensor)]
            self.rdfGraph.add((humidity, RDF.type, SOSA.Observation))
            self.rdfGraph.add((humidity, SOSA.isObservedBy,self.SENSOR[self.currentSensor]))
            self.rdfGraph.add((humidity, SOSA.hasSimpleResult, Literal(str(float(row["HMDT"])), datatype=XSD.float)))
            self.rdfGraph.add((humidity, SOSA.ObservableProperty,self.ROOM["{}#humidity".format(self.currentRoom)]))
            self.rdfGraph.add((humidity, SOSA.resultTime,Literal(date, datatype=XSD.dateTime)))
            

        #*****For Luminosity******
        if row["LUMI"] and math.isnan(row["LUMI"])==False:
            luminosity = self.OBSERVATION[str(idRow)+'is'+str(self.currentSensor)]
            self.rdfGraph.add((luminosity, RDF.type, SOSA.Observation))
            self.rdfGraph.add((luminosity, SOSA.isObservedBy,self.SENSOR[self.currentSensor]))
            self.rdfGraph.add((luminosity, SOSA.hasSimpleResult, Literal(str(float(row["LUMI"])), datatype=XSD.float)))
            self.rdfGraph.add((luminosity, SOSA.ObservableProperty,self.ROOM["{}#luminosity".format(self.currentRoom)]))
            self.rdfGraph.add((luminosity, SOSA.resultTime,Literal(date, datatype=XSD.dateTime)))
            
            

    # --------------------------This function made RDF triples and generate the whole RDF graphe ----------------------------------------------

    def generateTTL(self,csv,pathTo):


        self.rdfGraph.bind("room", self.ROOM)
        self.rdfGraph.bind("sensor", self.SENSOR)
        self.rdfGraph.bind("core", self.CORE)
        self.rdfGraph.bind("seas", self.SEAS)
        self.rdfGraph.bind("sosa", SOSA)
        self.rdfGraph.bind("ssn", SSN)
        self.rdfGraph.bind("rdf", RDF)
        self.rdfGraph.bind("xsd", XSD)
        self.rdfGraph.bind("observation", self.OBSERVATION)


        #the csv contains measurements for rooms and others components of this 4th floor that s why we make sur that we take the measuremeants for room and not something else
        room = self.csv['location'].str.contains('emse/fayol/e4/S4\w+')
        self.csv = self.csv[room]
        self.orderedAndFiltered = self.csv.sort_values(by=["id"])
        self.orderedAndFiltered = self.orderedAndFiltered.where(pd.notnull(self.orderedAndFiltered), None)
        j=0
        rdfG=None
        rdfG=copy.deepcopy(self.rdfGraph)
        for i, row in self.orderedAndFiltered.iterrows():
            #to generate the RDF it takes somehow significant time we found that if we split things it takes les time than generate it in the one that s why we split it to 10 files
            if (j%100000==0) or (j == (len(self.orderedAndFiltered)-1)  ) :
                self.rdfGraph.serialize(destination="RoomsTTLS/generated"+str(j)+".ttl")
                self.rdfGraph=copy.deepcopy(rdfG)
                print(j)
            
            if self.currentSensor == row["id"]:
                # Add sensor captured temperature
                self.sensorObserv(row=row, idRow=i)

            else:

                self.currentSensor = str(row["id"])

                #the replace from - to _ because the - can t be read by the RDF

                self.currentSensor = self.currentSensor.replace("-", "_")
                #print(self.currentSensor)
                sensor = str(row["location"])
                self.currentRoom = sensor.split("/")
                #Filter just Rooms from CSV as we already mentioned there is other buildings than rooms

                if len(self.currentRoom) == 4 :
                    self.currentRoom = self.currentRoom[3][1:]
                    self.rdfGraph.add((self.SENSOR[self.currentSensor], RDF.type, SOSA.Sensor))
                    #this triple aim to relate each room to its sensor by using the <https://w3id.org/rec/core/> vocabulary for the proprety isLocationOf
                    self.rdfGraph.add((self.ROOM[self.currentRoom], self.CORE.isLocationOf, self.SENSOR[self.currentSensor]))
                    self.sensorObserv(row=row, idRow=i)
                else:
                    #if the place isn't a room we won t add
                    self.currentSensor = None
            j=j+1
        print('Done')









rdf_Generator= Main()
meteo_extract =Meteo()

rdf_Generator.csv = rdf_Generator.csv_to_pd()
rdf_Generator.generateTTL(rdf_Generator.csv ,'')


meteo_extract.Met_toCSV("https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=15&mois2=10&annee2=2021","15_2021")
meteo_extract.Met_toCSV("https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=15&mois2=10&annee2=2021","16_2021")