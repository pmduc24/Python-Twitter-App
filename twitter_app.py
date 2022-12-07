import socket
import tweepy
from tweepy import StreamingClient
import json
import tkinter as tk
from pyspark.streaming import *
from pyspark.sql import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import threading
import matplotlib
matplotlib.rcParams.update(
    {
        'text.usetex': False,
        'font.family': 'stixgeneral',
        'mathtext.fontset': 'stix',
    }
)

ACCESS_TOKEN = '1591077341874515970-1fSKVAIWkKd47IpcsJDB2NRlSZIhO0'
ACCESS_SECRET = 'YxCSn6moKQ5apiit8nzNx0jtoZOJpnJuugkqtZrFVUTmN'
CONSUMER_KEY = 'FO3y71aFa4MfX6nvO5YhnlNjP'
CONSUMER_SECRET = 'aYVbM3cdNjexabfY6pLn2YR0nlXzZjZvhFFxQ3V7TWrlNNDHKX'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAGgKjwEAAAAAMpmu6w5ime5rkPXjyfLe%2Bk7AGNs%3DMuc1XocIorqBRVWFY4pjYkOL9yJpqkCpQejmEe3aR6i99ZeNc7'

# Key dung de truy cap vao API cua twitter


# client = tweepy.Client(access_token=ACCESS_TOKEN, access_token_secret=ACCESS_SECRET, consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET)
# auth = tweepy.OAuth1UserHandler(access_token=ACCESS_TOKEN, access_token_secret=ACCESS_SECRET, consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET)
# api = tweepy.API(auth)

#  khoi tao 

# **************************************************************************************************
class MyStream(StreamingClient): # class ke thua StreamingClient tu Twitter xu ly, tim kiem du lieu theo mong muon.

    def __init__(self, c_socket, bearer_token): # khoi tao doi tuong MyStream voi key
        super().__init__(bearer_token=bearer_token)
        self.client_socket = c_socket
      
    def on_data(self, data):
        try:
            data = data.decode('utf-8')
            # print(data)
            
            json_load = json.loads(data)
            id = json_load['data']['id']
            text = json_load['data']['text']
            self.client_socket.send(f"{text}".encode('utf-8'))
            out = str(id) + ": " + str(text)
            
            gui.db._draw_table(out + "\n")
            gui.db._draw_table("*" * 90 + "\n")
      
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_connect(self):
        print("You are now connected to the streaming API.")
        return True

    def on_tweet(self, tweet):
        print('/ontweet')
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print('ontweet/')
        print()
    
    def on_error(self, status_code):
        print("Status Code: " + str(status_code))
        print("Error: " + repr(status_code))

    def disconnect(self):
        return super().disconnect()

    def on_disconnect(self):
        print("Disconnect to the Stream")
        return super().on_disconnect()

    def filter(self, *, threaded=True, **params):
        return super().filter(threaded=threaded, **params)
            

# **************************************************************************************************
class Init: # class Init tao mot doi tuong MyStream

    def __init__(self,c_socket):
        self.stream = MyStream(c_socket,bearer_token=BEARER_TOKEN)
        gui.db.stop.config(command=self.stream.disconnect)
        
    def sendData(self): # gui thong tin ve cac tweet muon tra ve tu twitter
        try:
            rules = self.stream.get_rules()[0]
            self.stream.delete_rules(rules)
            print(self.stream.get_rules())
        except Exception as e:
            print(e)
        search = gui.filter.get('1.0', 'end-1c')

        self.stream.add_rules(tweepy.StreamRule(search))

        print(self.stream.get_rules())
        
        self.stream.filter()

    
    

# **************************************************************************************************
class Main: # khoi tao cong dich vu theo mo hinh client server gui du lieu nhan duoc tu twitter den spark
    host = "localhost"      # Get local machine name
    port = 5557                 # Reserve a port for your service.
    def __init__(self):
        self.s = socket.socket()         # Create a socket object
        self.s.bind((self.host, self.port))        # Bind to the port
        print("Listening on port: %s" % str(self.port))
        self.s.listen(5)                 # Now wait for client connection.
        self.c_socket, self.addr = self.s.accept()        # Establish connection with client.
        print("Received request from: " + str(self.addr))
        self.x = Init(self.c_socket)
        gui.filterButt.config(command=self.x.sendData)
        
        

# **************************************************************************************************
class Spark_App: # Khoi tao doi tuong spark
    def __init__(self):

    # Create DataFrame representing the stream of input lines from connection to localhost:5557
        self.spark = SparkSession.builder.getOrCreate()
        self.lines = self.spark.readStream.format("socket").option("host", "localhost").option("port", 5557).load()

    # Split the lines into words
        words = self.lines.select(explode(split(self.lines.value, " ")).alias("word"))

    # Select hashtag from words
        hashtags = words.where(words.word.startswith("#")).groupBy("word").count().sort(col("count").desc())

    # Start query on hashtags
        query = hashtags.writeStream.outputMode("complete").format("memory").queryName("hashtags").start()

        while(query.isActive): 
            top_10_tweets = self.spark.sql( 'Select * from hashtags' ).toPandas().head(10)
            x = top_10_tweets['word']
            y = top_10_tweets['count']
            gui._drawfigure(x,y)
            
            
        query.awaitTermination()
# **************************************************************************************************
class GUI(tk.Tk):
    def __init__(self):
        # Create link to csv file
        super().__init__()
        self.title("Spark Streaming")
        self.geometry("900x900")
        
        
        self.fig = plt.figure(figsize=(4, 5))
        self.canvas = FigureCanvasTkAgg(self.fig, self)
        # self.canvas.draw()
        self.canvas.get_tk_widget().place(relwidth = 0.5, relheight= 1,relx=0.5)
        self.toolbarFrame = tk.Frame(master=self)
        self.toolbarFrame.place(relwidth = 0.5, relheight= 0.05,relx=0.5)
        self.toolbar = NavigationToolbar2Tk(self.canvas, self.toolbarFrame)

        self.db = TextBox(self)

        self.filter = tk.Text(self)
        self.filter.place(relheight=0.05, rely=0.95, relwidth=0.45)
        self.filterButt = tk.Button(self, text= "filter")
        self.filterButt.place(relheight=0.05, rely=0.95, relwidth=0.05, relx=0.45)
        

    
    def _drawfigure(self,x,y):
        plt.clf()  # clear current figure
        plt.bar(x=x, height=y)  # plot the graph
        plt.xlabel("Hashtag")
        plt.ylabel("Count")
        plt.xticks(rotation=90)
        self.canvas.draw()  # refresh plot
        # self.canvas.flush_events()
        
        
# **************************************************************************************************  
class TextBox(tk.Text):
    def __init__(self, parent):
        super().__init__(parent)
        self.bind("<Key>", lambda e: "break")
        scroll_Y = tk.Scrollbar(self,orient="vertical", command=self.yview)
        self.configure(yscrollcommand=scroll_Y.set)
        scroll_Y.pack(side="right", fill="y")
        self.place(relwidth = 0.5, relheight= 0.95)

        

        self.clear = tk.Button(self, text = "Clear", cursor="arrow", command= lambda: self.delete('1.0', tk.END))
        self.clear.place(rely=0.95, relheight=0.05, relx= 0.5, relwidth= 0.2)
        self.stop = tk.Button(self, text = "Stop Stream", cursor="arrow")
        self.stop.place(rely=0.95, relheight=0.05, relx=0.3, relwidth=0.2)

    def _draw_table(self, text):        
        self.insert(tk.END, text)
        self.see(tk.END)
#  ***************************************************************************************************


def def1():
    Main()

def def2():
    Spark_App()

def def3():
    global gui
    gui = GUI()
    gui.mainloop()

try:
    t1 = threading.Thread(target=def1)
    t2 = threading.Thread(target=def2)
    t3 = threading.Thread(target=def3)
    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()
except:
    print("Error")
