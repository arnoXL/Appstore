# This file is used to analyze the data, which comes from the two json
# files in the repository, in MongoDB. It will give out 5 recommended apps
# for each app and 5 recommended apps for each user by using consine similarity
# and users' download history.

from pymongo import MongoClient
from dataservice import DataService
import operator
import time
import multiprocessing

# helper calss: use cosine similarity to caculate the similarity between
# two app lists
class Helper(object):
    @classmethod
    def cosineSimilarity(cls, appList1, appList2):
        temp = ((len(appList1) * len(appList2)) ** 0.5)
        return float(cls.__countMatch(appList1, appList2)) / temp

    @classmethod
    def __countMatch(cls, l1, l2):
        new1 = set(l1)
        new2 = set(l2)
        return len(new1.intersection(new2))


# used to calcaulate 5 recommended apps for each app
def calculateTop5(app, history, result):
    # the similarity between this app and other apps
    appSimi = {}

    for apps in history:
        similarity = Helper.cosineSimilarity([app], apps)
        for otherApp in apps:
            appSimi[otherApp] = appSimi.get(otherApp, 0) + similarity

    if not appSimi.has_key(app):
        return

    appSimi.pop(app)
    # sort the dict
    sortedTops = sorted(appSimi.items(), key = operator.itemgetter(1),
                        reverse = True)
    topFiveApp = [sortedTops[0][0], sortedTops[1][0], sortedTops[2][0],
                  sortedTops[3][0], sortedTops[4][0]]

    # add into the shared dictionary
    result[app] = topFiveApp


# used to calcaulate 5 recommended apps for each user
def calculateTop5ForUser(history, historyDict, userId, result):
    # the similarity between this app and other apps
    appSimi = {}
    # check if there is no history which is similar to the target history
    flag = False

    for key in historyDict.keys():
        similarity = Helper.cosineSimilarity(history, historyDict[key])
        if (not flag) and (similarity != 0):
            flag = True
        for app in historyDict[key]:
            appSimi[app] = appSimi.get(app, 0) + similarity

    # if there is no history which is similar to the target history
    if not flag:
        return

    # sort the dict
    sortedTops = sorted(appSimi.items(), key = operator.itemgetter(1),
                        reverse = True)
    topFiveApp = []
    count = 0
    for app in sortedTops:
        if app not in history:
            topFiveApp.append(app[0])
            count += 1
        if count == 5:
            break
    # print "top 5 app for " + str(userId) + ":\t" + str(topFiveApp)

    # add into MongoDB
    result[userId] = topFiveApp


# wrapper function of the calculateTop5()
def appWrapper(appList, history, result):
    for app in appList:
        calculateTop5(app, history, result)


# wrapper function of the calculateTop5ForUser()
def userWrapper(keys, history, result):
    for key in keys:
        temp = history.copy()
        del temp[key]
        calculateTop5ForUser(history[key], temp, key, result)


# main function
def main():
    noInput = True # weather user input a right command
    processes = []
    try:
        while noInput:
            flag = input("If you want to calculate each app's recommend apps" 
                         + " please press 1, If you want to calculate"
                         + " recommend apps for each user, please press"
                         + " 2: ")
            if flag == 1:
                noInput = False

                # start time
                start = time.clock()

                # set the shared dictionary
                rslt = {}
                result = multiprocessing.Manager().dict(rslt)

                # control the number of apps each process deals with
                count = 0

                # a list of appID
                keys = []

                # get client and init it with DataService class
                client = MongoClient("localhost", 27017)
                DataService.init(client)

                # start analyzing
                userDownloadHistory = DataService.retrieveUserDownloadHistory()
                allApp = DataService.retrieveAppInfo()

                length = len(allApp)
                for i in range(0, length, 500):
                    # each process will deal with 500 apps
                    p = multiprocessing.Process(target = appWrapper,
                        args = (allApp.keys()[i: min(i + 500, length)],
                            userDownloadHistory.values(), result))
                    processes.append(p)

                # set process to daemon and start
                for p in processes:
                    p.daemon = True
                    p.start()

                # main process will wait other processes
                for p in processes:
                    p.join()
                    
                # update MongoDB
                for key in result.keys():
                    DataService.updateAppInfo({"app_id": key},
                              {"$set": {"top 5 apps": result[key]}})
            elif flag == 2:
                noInput = False

                # start time
                start = time.clock()

                # set the shared dictionary
                rslt = {}
                result = multiprocessing.Manager().dict(rslt)

                # control the number of users each process deals with
                count = 0

                # a list of userID
                keys = []

                # get client and init it with DataService class
                client = MongoClient("localhost", 27017)
                DataService.init(client)

                # start analyzing
                userDownloadHistory = DataService.retrieveUserDownloadHistory()

                for key in userDownloadHistory.keys():
                    keys.append(key)
                    count += 1
                    # each process will deal with 500 users
                    if count == 500:
                        p = multiprocessing.Process(target = userWrapper,
                                args = (keys, userDownloadHistory, result))
                        processes.append(p)
                        keys = []
                        count = 0
                # the rest users
                p = multiprocessing.Process(target = userWrapper,
                        args = (keys, userDownloadHistory, result))
                processes.append(p)
                
                # set process to daemon and start
                for p in processes:
                    p.daemon = True
                    p.start()

                # main process will wait other processes
                for p in processes:
                    p.join()

                # update MongoDB
                for key in result.keys():
                    DataService.updateUserInfo({"user_id": key},
                          {"$set": {"top 5 recommended apps": result[key]}})
            else:
                print "Sorry, you press the wrong number, please try again."
    except Exception as e:
        print e
    finally:
        # close client
        if "client" in locals():
            client.close()
    end = time.clock()
    print "Finished! The elapsed time is " + str(end - start)


if __name__ == "__main__":
    main()