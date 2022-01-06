from flask import Flask, render_template
from datetime import datetime
from pyfuseki import FusekiQuery
from SPARQLWrapper import SPARQLWrapper , JSON
import pandas as pd

app = Flask(__name__)



def compareWithMeteo(day):
    
    csvdata=None

    data15 = pd.read_csv("../RDFGen/data/15_2021.csv")
    data16 = pd.read_csv("../RDFGen/data/16_2021.csv")
    
    
    if int(day[8:10])==15:
        print('ok1')
        csvdata=data15
    if int(day[8:10])==16:
        print('ok2')
        csvdata=data16
    print("////////////////////////////////////")

    
    data4=[]
    
    
    for i, elm in csvdata.iterrows():
        #print(elm)
        t = float(elm["temperature"])
        h = int(elm["hour"])
        
        print(int(day[11:13]))

        if h==int(day[11:13]):
            if(h<10):
                h="0"+str(h)
                print('')
                print(str(h))
                

            #d1=datetime(year=2021, month=11, day=15, hour=h,minute=00, second=00).strftime("%Y-%m-%dT%H:%M:%S")
            #d2=datetime(year=2021, month=11, day=15, hour=h,minute=59, second=59).strftime("%Y-%m-%dT%H:%M:%S")
            ##all  
            sparql_str4 = """
            
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        PREFIX sensor: <http://localhost:3030/sensor/>
                        PREFIX sosa: <http://www.w3.org/ns/sosa/>
                        PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                        PREFIX core: <https://w3id.org/rec/core/>
                        PREFIX ssn: <http://www.w3.org/ns/ssn/>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
						PREFIX observation: <http://localhost:3030/observation/>
                        
                        SELECT ?room ?d  (avg(?result) as ?hourAvgTemp ) (?hourAvgTemp-(xsd:float('"""+str(t)+"""')) as ?diff) ?time
                                                        WHERE {
 ?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; sosa:ObservableProperty ?observableproperty.
                                                        FILTER (
                                                        (regex(str(?observableproperty),"temperature")  &&
                            (xsd:dateTime(?time) = '"""+str(day)+":00:00"+"""'^^xsd:dateTime)
                            ))                  
                        BIND( SUBSTR(str(?datetime), 1, 13) AS ?d )    
                        BIND(CONCAT(STR( ?d ), ":00:00" ) AS ?time ) 
                        
                        }
                        groupby ?d ?room ?time  
            
                            """
                            
                            
                            
            print('//////////////Requete')
            print(sparql_str4)

            sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")

            sparql.setQuery(sparql_str4)



            sparql.setReturnFormat(JSON)
            results4 = sparql.query().convert()


            print('new line')

            #print(results4["results"]["bindings"][0])

            for result in results4["results"]["bindings"]:
                result2=(result["room"]["value"]).split("/")
                #data.append({'s':result["s"],'p':result["p"],'o':result["o"],})
                data4.append({'room':result2[7],'time':result["time"]["value"],'temperature':result["hourAvgTemp"]["value"],'diff':float(result["diff"]["value"])})
            print('///////////////////////////////////////////////////////')
            print(data4)
        
        
    return data4
        
    




@app.route('/')
def index():
    
    return render_template('index.html')



@app.route('/room<room>')
def room(room):
    sparql_str ="""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX sensor: <http://localhost:3030/sensor/>
                    PREFIX sosa: <http://www.w3.org/ns/sosa/>
                    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>
                    PREFIX ssn: <http://www.w3.org/ns/ssn/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
					PREFIX observation: <http://localhost:3030/observation/>
                    SELECT ?observableproperty
                                WHERE {
             ?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; sosa:ObservableProperty ?observableproperty.
                                FILTER (
        regex(str(?room),'"""+room+"""')
                                )

                                }GROUP BY ?observableproperty 
    
                                """
    
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    results2 = sparql.query().convert()



    data=[]    
    print("rss")
    print(results2)
  

    for result in results2["results"]["bindings"]:
        
        
        result2=(result["observableproperty"]["value"]).split("/")
        result22=(result2[7]).split("#")
        
        data.append({'prop':result22[1]})
    
    print(data)

##
    


    return render_template("room.html",room=room,rows=data)




@app.route('/temperature<room>')
def tmperature(room):
    sparql_str ="""

                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        PREFIX sensor: <http://localhost:3030/sensor/>
                        PREFIX sosa: <http://www.w3.org/ns/sosa/>
                        PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                        PREFIX core: <https://w3id.org/rec/core/>
                        PREFIX ssn: <http://www.w3.org/ns/ssn/>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
						PREFIX observation: <http://localhost:3030/observation/>

                    SELECT ?room ?datetime ?result
                                WHERE {
 								?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; 
                                sosa:ObservableProperty ?observableproperty.
                                FILTER (
                                (regex(str(?observableproperty),"temperature") &&  regex(str(?room),'"""+room+"""')
                                
                                
  
    ))
                                            
                                }
                                """
    
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    resultsT = sparql.query().convert()



    dataT=[]    
    print("rss")
    print(resultsT["results"]["bindings"][0])
  

    for result in resultsT["results"]["bindings"]:
        
        
        resultT=(result["room"]["value"]).split("/")
        resultTT=(resultT[7])
        
        dataT.append({'room':resultTT,'Time':result["datetime"]["value"],'Temperature':result["result"]["value"]})
    print("sssssssssss")
    print(dataT[0])   


    return render_template("temperature.html",room=room,rows=dataT)




@app.route('/humidity<room>')
def humidity(room): 
    sparql_str ="""
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX sensor: <http://localhost:3030/sensor/>
                    PREFIX sosa: <http://www.w3.org/ns/sosa/>
                    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>
                    PREFIX ssn: <http://www.w3.org/ns/ssn/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    PREFIX observation: <http://localhost:3030/observation/>
                    SELECT ?room ?datetime ?result
                                WHERE {
                                ?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; 
                                sosa:ObservableProperty ?observableproperty.
                                FILTER (
                                (regex(str(?observableproperty),"humidity") &&  regex(str(?room),'"""+room+"""')
                                
  
    ))
                                            
                                }
                                """
    
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    resultsT = sparql.query().convert()

    dataT=[]    
    print("rss")
    print(resultsT["results"]["bindings"][0])
  

    for result in resultsT["results"]["bindings"]:
        
        
        resultT=(result["room"]["value"]).split("/")
        resultTT=(resultT[7])
        
        dataT.append({'room':resultTT,'Time':result["datetime"]["value"],'Humidity':result["result"]["value"]})
    print("sssssssssss")
    print(dataT[0])   
    
    return render_template("humidity.html",room=room,rows=dataT)





@app.route('/luminosity<room>')
def luminosity(room):
    sparql_str ="""
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX sensor: <http://localhost:3030/sensor/>
                    PREFIX sosa: <http://www.w3.org/ns/sosa/>
                    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>
                    PREFIX ssn: <http://www.w3.org/ns/ssn/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    PREFIX observation: <http://localhost:3030/observation/>
                    SELECT ?room ?datetime ?result
                                WHERE {
                                ?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; 
                                sosa:ObservableProperty ?observableproperty.
                                FILTER (
                                (regex(str(?observableproperty),"luminosity") &&  regex(str(?room),'"""+room+"""')
                                
  
    ))
                                            
                                }
                                """
    
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    resultsT = sparql.query().convert()



    dataT=[]    
    print("rss")
    print(resultsT["results"]["bindings"][0])
  

    for result in resultsT["results"]["bindings"]:
        
        
        resultT=(result["room"]["value"]).split("/")
        resultTT=(resultT[7])
        
        dataT.append({'room':resultTT,'Time':result["datetime"]["value"],'Luminosity':result["result"]["value"]})
    print("sssssssssss")
    print(dataT[0])   


    return render_template("luminosity.html",room=room,rows=dataT)




@app.route('/extreme')
def extreme():
    sparql_str ="""
          PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX sensor: <http://localhost:3030/sensor/>
                    PREFIX sosa: <http://www.w3.org/ns/sosa/>
                    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>
                    PREFIX ssn: <http://www.w3.org/ns/ssn/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    SELECT  ?day
                                                    WHERE {
                 ?room core:isLocationOf ?sensor .
                                ?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime; sosa:ObservableProperty ?observableproperty.


                    BIND( SUBSTR(str(?datetime), 1, 13) AS ?d )    
                    BIND(CONCAT(STR( ?d ), ":00:00" ) AS ?time ) 
                    BIND(CONCAT(STR(YEAR(xsd:dateTime(?time))),'-',STR(MONTH(xsd:dateTime(?time))),'-',STR(DAY(xsd:dateTime(?time)))) as ?day)
                    
                    }
                    groupby  ?day 
                    order by ?day

                                """
                                
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    resultsT = sparql.query().convert()



    data=[]    
    ##print("rss")
    #print(resultsT["results"]["bindings"][0])
  

    for result in resultsT["results"]["bindings"]:
        
        
        #resultT=(result["room"]["value"]).split("/")
        #resultTT=(resultT[7])
        
        data.append({'day':result["day"]["value"]})
    print("sssssssssss")
    print(data)
    
    
    return render_template("extreme.html",rows=data)





@app.route('/exthour<day>')
def exthour(day):
    print(str(day)+"T00:00:00")
    sparql_str ="""
    
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX sensor: <http://localhost:3030/sensor/>
                    PREFIX sosa: <http://www.w3.org/ns/sosa/>
                    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>
                    PREFIX ssn: <http://www.w3.org/ns/ssn/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    SELECT  ?time   ( DAY(xsd:dateTime(?time)) as ?day)
                                                    WHERE {
                                                    ?room core:isLocationOf ?sensor .
            										?observation sosa:isObservedBy ?sensor ;sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime;sosa:ObservableProperty ?observableproperty.

                    filter(
                        DAY(xsd:dateTime(?time)) = DAY(xsd:dateTime('"""+ str(day)+"T00:00:00" +"""'))
                    )
                    BIND( SUBSTR(str(?datetime), 1, 13) AS ?d )    
                    BIND(CONCAT(STR( ?d ), ":00:00" ) AS ?time ) 
                    
                    }
                    groupby  ?time
                    order by ?time
                                """
                                
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")
    sparql.setQuery(sparql_str)


    sparql.setReturnFormat(JSON)
    resultsT = sparql.query().convert()



    data=[]    
    ##print("rss")
    #print(resultsT["results"]["bindings"][0])
  

    for result in resultsT["results"]["bindings"]:
        
        
        #resultT=(result["room"]["value"]).split("/")
        #resultTT=(resultT[7])
        
        data.append({'day':result["time"]["value"]})
    print("/////////////////////////////////////")
           
                                
    
    
    
    return render_template("exthour.html",rows=data,day=day)





@app.route('/classified<day>')
def classified(day):
    
    data=compareWithMeteo(day)
    return render_template("classified.html",rows=data,day=day)




@app.route('/roomes')
def roomes():
    
    sparql_str2 = """
    PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                    PREFIX core: <https://w3id.org/rec/core/>

    SELECT ?room 
                                WHERE {
                                ?room core:isLocationOf ?sensor 
    }
    """
    
    sparql = SPARQLWrapper("http://localhost:3030/SemWebProject_data")  
    
    sparql.setQuery(sparql_str2)


    sparql.setReturnFormat(JSON)
    results2 = sparql.query().convert()



    data2=[]    
    data22=[]
  

    for result in results2["results"]["bindings"]:
        
        
        result2=(result["room"]["value"]).split("/")
        
        data2.append({'room':result2[7]})
        data22.append({'room':result["room"]["value"]})

    #print(data2[0])
    return render_template("roomes.html",rooms=data2)