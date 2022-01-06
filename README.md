Semantic web APP with Flask 

Pre-requirements
- Create database 'SemWebProject_data' on Fuseki 
- Download with command line the .ttl files from terretoire platforme using wget -r -A .ttl https://territoire.emse.fr/kg/ and upload it to fuseki 
- Upload  20211116-daily-sensor-measures.csv to /data folder (we did not put on github because it s too big)
- run the main.py to generate the observations RDF 
- Upload the .ttl files into fuseki database

Run the Flask up with these steps
- Create an Virtual enviroment, access to it and install all the dependencies :
  - py -m venv venv
  - venv/scripts/activate
  - set FLASK_APP=app.py
  - pip install flask .....
  - flask run
