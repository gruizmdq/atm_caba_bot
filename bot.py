import csv 
import requests 
from collections import defaultdict
import math
from pymongo import MongoClient
import os.path
import json
from bson import json_util
import threading
import datetime
import time as Time
from telegram.ext import Updater, CommandHandler, Filters, MessageHandler

LIMIT_EXTRACTIONS = 1000
MIN_DISTANCE = 500

################################
# PERSISTENCE CLASS to support shutdowns
###############################
class Persistence():
    DB_HOST = 'localhost'
    DB_PORT = 27017
    DB_NAME = 'jampp'
    COLLECTION_ATMS = 'atms'
    
    db = MongoClient(DB_HOST, DB_PORT)[DB_NAME]

    @staticmethod   
    def get_atms():
        result = list(Persistence.db[Persistence.COLLECTION_ATMS].find())
        return result
    @staticmethod   
    def insert_atms(atms):
        result = Persistence.db[Persistence.COLLECTION_ATMS].insert_many(json.loads(atms))
        return result.acknowledged
    @staticmethod   
    def update_extraction(id_cajero, limite_recarga):
        result = Persistence.db[Persistence.COLLECTION_ATMS].update_one({'id':id_cajero},{"$set":{'recargas':limite_recarga}})
        return result.acknowledged
    @staticmethod
    def update_amts():
        result = Persistence.db[Persistence.COLLECTION_ATMS].update_many({}, {"$set":{'recargas':LIMIT_EXTRACTIONS}})
        return result.acknowledged


################################
# ATM CLASS to make easier fields access
###############################
class Atm():
    def __init__(self,id_cajero, longitud, lat, banco, red, calle, altura, barrio, comuna, limite_recarga=1000):
        self.id = id_cajero
        self.long = longitud
        self.lat = lat
        self.banco = banco
        self.red = red
        self.calle = calle
        self.altura = altura
        self.barrio = barrio
        self.comuna = comuna
        self.distance = 0
        self.limite_recarga = limite_recarga
        
    def __repr__(self):
        return self.red+", "+self.calle+" "+self.altura +", "+ self.comuna + ", "+ self.barrio + ", distance: "+str(self.distance) + ', recargas: '+ str(self.limite_recarga)
    

    def set_distance(self,distance):
        self.distance = distance


################################
# CONTROLLER CLASS
###############################

class Controller():

    def __init__(self, COMUNAS_NEIGHBOURS):
        self.atm_lock = threading.RLock()
        self.atms = self.get_atms()
        self.bot_request = {}
        self.t = threading.Thread(target=self.restore_atms)
        self.t.start()
        self.COMUNAS_NEIGHBOURS = COMUNAS_NEIGHBOURS

    #Returns the distance in meters from two coords
    def distance_between_coords(self,long1, lat1, long2, lat2):
        earth_radius = 6371 * 1000
        dLat = math.radians(lat2-lat1)
        dLon = math.radians(long2-long1)

        lat1 = math.radians(lat1)
        lat2 = math.radians(lat2)

        a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2); 
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)); 
        return earth_radius * c

    #Get atms from database or from csv file
    def get_atms(self):
        try: 
            self.atm_lock.acquire()
            atms = self.get_atms_from_database()
            if len(atms) == 0:
                atms = self.get_atms_from_csv('cajeros-automaticos.csv')
            self.atm_lock.release()
            return atms
        except Exception as e:
            print(str(e))

    #Get atms from database if they exist
    def get_atms_from_database(self):
        atms = defaultdict(lambda:defaultdict(list))
        r = Persistence.get_atms()
        for i in range(len(r)):
            atms[r[i]['red']][r[i]['comuna']].append(Atm(r[i]['id'],r[i]['long'],r[i]['lat'],r[i]['banco'],r[i]['red'],r[i]['calle'],r[i]['altura'],r[i]['barrio'],r[i]['comuna'],r[i]['recargas']))
        return atms

    #Get atms from csv file. If not exists, It will download file from CABA server. Then persists data
    def get_atms_from_csv(self, file):
        array_to_persist = []

        #download file
        if not os.path.exists(file):
            self.get_csv()
        atms = defaultdict(lambda:defaultdict(list))

        with open(file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            for row in csv_reader:
                #Filtra CABA
                if (row[6] == 'CABA'):
                    array_to_persist.append({'id':row[0],'long':row[1],'lat':row[2],'banco':row[3],'red':row[4],'calle':row[10],'altura':row[11],'barrio':row[13],'comuna':row[14],'recargas':1000})
                    self.atms[row[4]][row[14]].append(Atm(row[0],row[1], row[2], row[3], row[4], row[10], row[11], row[13], row[14]))
        try:
            Persistence.insert_atms(json.dumps(array_to_persist))
            del array_to_persist
        except Exception as e:
            print(str(e))
        return atms

    #Find atms
    # red = ['BANELCO', 'LINK']
    # comuna: comuna where user is located.
    # lng, lat are the location in coords
    # result array: it is an array with previous results, because method will be called many time for neighbours comunas
    def find_atm(self, red, comuna, lng, lat, result=[], min_distance=MIN_DISTANCE):
        for i in self.atms[red][comuna]:
            distance = self.distance_between_coords(float(i.long), float(i.lat), lng, lat)
            if (distance < min_distance and i.limite_recarga >= 1):
                i.set_distance(distance)
                if (len(result)<3):
                    result.append(i)
                else:
                    maximun_distance = 0
                    index = 0
                    index_to_remove = 0
                    for r in result:
                        if(r.distance > maximun_distance):
                            maximun_distance = r.distance
                            index_to_remove = index
                        index += 1
                    result[index_to_remove] = i  
                    min_distance = max([result[0].distance, result[1].distance, result[2].distance]) 
        return sorted(result, key=lambda x: x.distance, reverse=False), min_distance

    #Update atm extraction limit
    def add_extraction(self, r):
        #The exercise does not say nothing when the number of atm is 2

        first = 0.7
        second = 0.2
        third = 0.1
        arr = [first, second, third]

        #To avoid dirty read
        self.atm_lock.acquire()
        
        if len(r) == 1:
            r[0].limite_recarga -= 1
        elif len(r) == 2:
            r[0].limite_recarga -= .8
            r[1].limite_recarga -= .2
        else:
            for i in range(len(r)):
                r[i].limite_recarga -= arr[i]

        self.atm_lock.release()

    #Persists update
    def update_extractions(self, r):
        for i in range(len(r)):
            try:
                Persistence.update_extraction(r[i].id, r[i].limite_recarga)
            except Exception as e:
                print(str(e))

    def get_csv(self):
        r = requests.get('http://cdn.buenosaires.gob.ar/datosabiertos/datasets/cajeros-automaticos/cajeros-automaticos.csv')
        with open('cajeros-automaticos.csv', 'wb') as f:
            f.write(r.content)

    #Thread to set atm extraction limit to 1000 at 8:00am
    def restore_atms(self):
        while True:
            time = datetime.datetime.now()
            hour_to_restore = datetime.datetime.strptime('08:00','%H:%M').time()
            
            if time.time() > hour_to_restore:
                delta_day = 1
                if time.weekday() == 5:
                    delta_day = 2
                elif time.weekday() == 4:
                    delta_day = 3
                time_to_next_update = (datetime.datetime(time.year,time.month,time.day,8,0)+ datetime.timedelta(days=delta_day))-time
                #Time.sleep(time_to_next_update.seconds)
                #delete this and uncomment above line
                Time.sleep(2)
                self.atm_lock.acquire()
                self.update_amts()
                
                self.atm_lock.release()    

    def update_amts(self):
        try: 
            Persistence.update_amts()
            self.atms = self.get_atms()
        except Exception as e:
            print(str(e))

    
    def get_comuna_from_location(self, lat, lng):

        # CABA api to get data from location
        URL = "http://ws.usig.buenosaires.gob.ar/datos_utiles/"
        
        # defining a params dict for the parameters to be sent to the API 
        PARAMS = {'x':lng, 'y': lat} 
    
        # sending get request and saving the response as response object 
        r = requests.get(url = URL, params = PARAMS) 
    
        # extracting data in json format 
        data = r.json() 
        return data['comuna']

    #############
    # BOT METHODS
    #############

    def start(self, bot, update):
        bot.send_message(chat_id=update.message.chat_id, text="Welcome!. To get the nearest atms from your location, please send /BANELCO or /LINK")

    def get_red(self, bot, update):
        self.bot_request[update.message.chat_id] = update.message.text[1:].upper()
        bot.send_message(chat_id=update.message.chat_id, text="We need your location to share you the nearest "+update.message.text.upper()[1:]+" atms. Please, share it")

    def location(self, bot, update):
        message = update.message
        red = self.bot_request.get(message.chat_id)
        if red is None:
            bot.send_message(chat_id=update.message.chat_id, text="Please, you need to specify /LINK or /BANELCO first")
        else:
            red = self.bot_request.pop(message.chat_id)
            comuna = self.get_comuna_from_location(message.location.latitude, message.location.longitude)
            if comuna == '':
                bot.send_message(chat_id=update.message.chat_id, text="You are not in CABA")
            else: 
                bot.send_message(chat_id=update.message.chat_id, text="You are in "+comuna)
                result, image = self.proccess(red, comuna,message.location.longitude,message.location.latitude)
                for i in result:
                    bot.send_message(chat_id=update.message.chat_id, text=str(i))
                if len(result) > 0:
                    bot.send_photo(chat_id=update.message.chat_id, photo=image)
                else:
                    bot.send_message(chat_id=update.message.chat_id, text='There is not atm near you, sory')


    ###########
    # Main method to find closest atms
    ###########
    def proccess(self,red, comuna, lng, lat):
        r,d = self.find_atm(red,comuna,lng,lat,[])
        #Search in other comunas
        num_comuna = comuna[-2:]
        i = 0
        neighbours = self.COMUNAS_NEIGHBOURS[int(num_comuna)-1]

        changed = False
        while(i < len(neighbours) and (not changed or len(r) < 3)):
            aux = r
            r,d = self.find_atm(red,neighbours[i],lng,lat, r, d)
            #Found atm in other comuna. So, I can assume that will not be necessary search in other comuna.
            if aux != r:
                changed =True
            i += 1

        #Some atms do not have Comuna field.
        r,d = self.find_atm(red,'',lng,lat, r, d)

        print(r)
        self.add_extraction(r)
        self.update_extractions(r)

        params = ''
        for i in r:
            params +='&markers=color:red%7C'+i.lat+','+i.long

        image = 'https://maps.googleapis.com/maps/api/staticmap?center='+f'{lat:.9g}'+','+f'{lng:.9g}'+'&zoom=14&size=600x300&maptype=roadmap&markers=color:blue%7C'+f'{lat:.9g}'+','+f'{lng:.9g}'+params+'&key=AIzaSyD1rxLVzd_wUtX_DORZQazOAJoUnglxh2s'

        return r, image

################################
#START THE API
###############################
if __name__ == "__main__":

    
    #Do not use API keys for local or production applications!!!!!!!!!!!
    API_KEY = ''
    TELEGRAM_API_KEY = ''

    #COMUNAS neighbours
    COMUNA1 = ['Comuna 2', 'Comuna 3', 'Comuna 4']
    COMUNA2 = ['Comuna 1', 'Comuna 3', 'Comuna 5', 'Comuna 14']
    COMUNA3 = ['Comuna 1', 'Comuna 2', 'Comuna 4', 'Comuna 5']
    COMUNA4 = ['Comuna 1', 'Comuna 3', 'Comuna 5', 'Comuna 6', 'Comuna 7', 'Comuna 8']
    COMUNA5 = ['Comuna 2', 'Comuna 3', 'Comuna 4', 'Comuna 6', 'Comuna 7', 'Comuna 14', 'Comuna 15']
    COMUNA6 = ['Comuna 5', 'Comuna 7', 'Comuna 11', 'Comuna 15']
    COMUNA7 = ['Comuna 4', 'Comuna 5', 'Comuna 6', 'Comuna 8', 'Comuna 9', 'Comuna 10', 'Comuna 11']
    COMUNA8 = ['Comuna 4', 'Comuna 7', 'Comuna 9']
    COMUNA9 = ['Comuna 7', 'Comuna 8', 'Comuna 10']
    COMUNA10 = ['Comuna 7', 'Comuna 9', 'Comuna 11']
    COMUNA11 = ['Comuna 6', 'Comuna 7', 'Comuna 10', 'Comuna 12', 'Comuna 15']
    COMUNA12 = ['Comuna 11', 'Comuna 13', 'Comuna 14']
    COMUNA13 = ['Comuna 12', 'Comuna 14', 'Comuna 15']
    COMUNA14 = ['Comuna 2', 'Comuna 5', 'Comuna 13', 'Comuna 15']
    COMUNA15 = ['Comuna 5', 'Comuna 6', 'Comuna 11', 'Comuna 12', 'Comuna 13', 'Comuna 14']

    COMUNAS_NEIGHBOURS = [COMUNA1, COMUNA2, COMUNA3, COMUNA4, COMUNA5, COMUNA6, COMUNA7, COMUNA8, COMUNA9, COMUNA10, COMUNA11, COMUNA12, COMUNA13, COMUNA14, COMUNA15]
    
    controller = Controller(COMUNAS_NEIGHBOURS)

    updater = Updater(token=TELEGRAM_API_KEY)
    dispatcher = updater.dispatcher

    start_handler = CommandHandler('start', controller.start)
    dispatcher.add_handler(start_handler)

    banelco_handler = CommandHandler('banelco', controller.get_red)
    dispatcher.add_handler(banelco_handler)

    link_handler = CommandHandler('link', controller.get_red)
    dispatcher.add_handler(link_handler)

    location_handler = MessageHandler(Filters.location, controller.location, edited_updates=True)
    dispatcher.add_handler(location_handler)

    updater.start_polling()

    
