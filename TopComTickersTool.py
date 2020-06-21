import nltk

from webull import webull
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()


# self.wb = webull()
# self.wb.login('joseph.h.cottingham@gmail.com', 'Fold2706metro39ted')
# x = self.wb.get_quote(tId='913255598')
# print(x)
# print('logged in')
# self.wb.get_social('');

# 11734167054323712
# bullish

class TopComTickersTool:
    def __init__(self):
        self.wb = webull()
        print('TopComTickersTool Meathods')
        print('findStringValues()')

    @staticmethod
    def findStringValues(open, close, message):
        values = []
        startIndex = message.find(open, 0)
        endIndex = message.find(close, startIndex)
        while startIndex != -1 and endIndex != -1:
            values.append(message[startIndex+len(open):endIndex])
            message = message[endIndex:]
            startIndex = message.find(open, 0)
            endIndex = message.find(close, startIndex)
        return values

    @staticmethod
    def isPositive(txt):
        sentiment = sia.polarity_scores(txt)['compound']
        if sentiment > 0:
            return True
        else:
            return False

    def createTickerList(self, filePath, TopicCSVPath):
        if filePath is None or filePath.split('.')[-1] != 'csv':
            filePath='TickerVals.csv'
        topicIdFile=pd.read_csv(TopicCSVPath)
        tickerIdFile=pd.DataFrame()

        currentTopicNum = -1
        totalTopicNum = len(topicIdFile.index)
        for topicId in topicIdFile['ID']:
            currentTopicNum+=1
            posts = self.wb.get_social_posts(topicId)
            currentPostNum = -1
            currentPostNum = len(posts)
            for post in posts:
                currentPostNum+=1
                print("Current Topic: {0} / {1}".format(currentTopicNum, totalTopicNum))
                print("Current Post: {0} / {1}".format(currentPostNum, currentPostNum))

                txt = post['content']['txt']

                for key in post['link'].keys():
                    if key == 'tickers':
                        tickers = post['link']['tickers']
                    else:
                        ticker = []

                for ticker in tickers:
                    tickerID = ticker['tickerId']
                    name = ticker.get('name', 'N/A')
                    symbol = ticker.get('symbol', 'N/A')
                    tickerIdRecorded = False
                    tickerIdKeyExists = any(tickerIdFile.keys() == 'TickerID')
                    if tickerIdKeyExists:
                        tickerIdRecorded = any(tickerIdFile['TickerID'] == tickerID)
                    if tickerIdKeyExists and not tickerIdRecorded:
                        try:
                            if TopComTickersTool.isPositive(txt):
                                tickerIdFile = tickerIdFile.append(pd.DataFrame([{'TickerID':tickerID,'Symbol':symbol, 'Name':name, 'Occurrence': 1, 'Positive': 1, 'Negative': 0}]))
                            else:
                                tickerIdFile = tickerIdFile.append(pd.DataFrame([{'TickerID':tickerID,'Symbol':symbol, 'Name':name, 'Occurrence': 1, 'Positive': 0, 'Negative': 1}]))

                            tickerIdFile = tickerIdFile.reset_index(drop=True)
                            print('New Ticker Added')
                        except Exception as e:
                            print(e.with_traceback())
                    elif tickerIdKeyExists and tickerIdRecorded:
                        index = tickerIdFile[tickerIdFile['TickerID'] == tickerID].index.values.astype(int)
                        tickerIdFile.at[int(index), 'Occurrence'] = (int(tickerIdFile.at[int(index), 'Occurrence'])+1)

                        if TopComTickersTool.isPositive(txt):
                            tickerIdFile.at[int(index), 'Positive'] = (int(tickerIdFile.at[int(index), 'Positive']) + 1)
                        else:
                            tickerIdFile.at[int(index), 'Negative'] = (int(tickerIdFile.at[int(index), 'Negative']) + 1)

                        print('New Occurrence Added')
                    elif not tickerIdKeyExists:
                        if TopComTickersTool.isPositive(txt):
                            tickerIdFile = pd.DataFrame([{'TickerID': tickerID, 'Symbol': symbol, 'Name': name, 'Occurrence': '1', 'Positive': 1, 'Negative': 0}])
                        else:
                            tickerIdFile = pd.DataFrame([{'TickerID': tickerID, 'Symbol': symbol, 'Name': name, 'Occurrence': '1', 'Positive': 0, 'Negative': 1}])

        tickerIdFile.to_csv(filePath, mode='w', header=True)
        print("Done")


    def createIdList(self, filePath, TopicID, Topic):
        if filePath is None or filePath.split('.')[-1] != 'csv':
            filePath='IdVals.csv'
        IdFile=pd.DataFrame([{'ID':TopicID,'Name':Topic}])
        currentRow = -1
        while currentRow != IdFile.index.values[-1]:
            currentRow+=1
            print()
            print()
            print('-----------------------------------------')
            print()
            print('Current Row: ' + str(currentRow) + ' out of ' + str(IdFile.index.values[-1]))
            num=0
            for message in self.wb.get_social_posts(IdFile.at[currentRow, 'ID']):
                newIdArray = TopComTickersTool.findStringValues('<C|', '>', message['content']['txt'])
                print(newIdArray)
                num += 1
                print('Message Num:' + str(num))
                for newId in newIdArray:
                    if not any(IdFile['ID']==newId):
                        try:
                            message = self.wb.get_social_home(newId, 1)
                            name = message['content']['title']
                            IdFile = IdFile.append(pd.DataFrame([{'ID':newId,'Name':name}]))
                            IdFile = IdFile.reset_index(drop=True)
                        except:
                            print(message)

        IdFile.to_csv(filePath, mode='w', header=True)
        print("Done")