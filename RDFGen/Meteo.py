import pandas as pd


def FixHour(Hour):
    return pd.to_numeric(Hour[:-1]) 

def FixTemperature(Temp):
    return pd.to_numeric(Temp[:-2]) 
 
def FixHumidity(Hum):
    return pd.to_numeric(Hum[:-1]) 
class Meteo():
    def __init__(self):
        self.url1=None
        self.url2=None
    
    def Met_toCSV(self,url,day):
        dfs = pd.read_html(url)
        df=dfs[4]
        _masque=df[0].str.contains('^\d+ h', na=False)
        df=df[_masque]
        df=df[[0,4,5]]
        df.columns=["hour","temperature","humidity"]

        #df["hour"]= df["hour"][:-1]




        #df['add'] = df.apply(lambda row : add(row['A'],row['B'], row['C']), axis = 1)

        df["hour"]= df.apply(lambda row : FixHour(row["hour"]), axis = 1)
        df["temperature"]= df.apply(lambda row : FixTemperature(row["temperature"]), axis = 1)
        df["humidity"]= df.apply(lambda row : FixHumidity(row["humidity"]), axis = 1)

        print(df.dtypes)
        print(df)

        #pd.to_numeric(df["hour"])


        #print(df.dtypes)
        print("okkkk"+str(df["hour"].iloc[0])+"okkkk"+str(df["temperature"].iloc[0])+'okkkk'+str(df["humidity"].iloc[0])+'okkkk')
        df.to_csv("data/{}.csv".format(day),header= ["hour","temperature","humidity"] , index=False)

        #df.to_csv("./{}.csv".format(day),header= self.columns , index=False)
