from pymongo import MongoClient
from dataservice import DataService
import operator
import time
import multiprocessing

class Helper(object):
    @classmethod
    def cosineSimilarity(cls, appList1, appList2):
        temp = ((len(appList1) * len(appList2)) ** 0.5)
        return float(cls.__countMatch(appList1, appList2)) / temp

    @classmethod
    def __countMatch(cls, l1, l2):
        # count = 0
        # for a in l1:
        #     if a in l2:
        #         count += 1
        # return count
        new1 = set(l1)
        new2 = set(l2)
        return len(new1.intersection(new2))

def calculateTop5(app, history):
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
    # print "top 5 app for " + str(app) + ":\t" + str(topFiveApp)

    # add into MongoDB
    DataService.updateAppInfo({"app_id": app},
                              {"$set": {"top 5 apps": topFiveApp}})


def calculateTop5ForUser(history, historyDict, userId):
    # client = MongoClient("localhost", 27017)
    # DataService.init(client)

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

    # topFiveApp = [sortedTops[0][0], sortedTops[1][0], sortedTops[2][0],
    #               sortedTops[3][0], sortedTops[4][0]]

    # print "top 5 app for " + str(app) + ":\t" + str(topFiveApp)

    # add into MongoDB
    DataService.updateUserInfo({"user_id": userId},
                              {"$set": {"top 5 apps": topFiveApp}})

def main():
    noInput = True
    processes = []
    try:
        while noInput:
            flag = input("If you want to calculate each app's recommend apps" 
                         + " please press 1, If you want to calculate"
                         + " recommend apps for each user, please press"
                         + " 2: ")
            if flag == 1:
                noInput = False
                start = time.clock()

                # get client and init it with DataService class
                client = MongoClient("localhost", 27017)
                DataService.init(client)

                # start analyzing
                userDownloadHistory = DataService.retrieveUserDownloadHistory()

                allApp = DataService.retrieveAppInfo()
                for app in allApp.keys():
                    calculateTop5(app, userDownloadHistory.values())
            elif flag == 2:
                noInput = False
                start = time.clock()

                # get client and init it with DataService class
                client = MongoClient("localhost", 27017)
                DataService.init(client)

                # start analyzing
                userDownloadHistory = DataService.retrieveUserDownloadHistory()

                for key in userDownloadHistory.keys():
                    temp = userDownloadHistory.copy()
                    del temp[key]
                    
                    # p = multiprocessing.Process(target = calculateTop5ForUser,
                    #             args = (userDownloadHistory[key], temp, key))
                    # processes.append(p)

                    calculateTop5ForUser(userDownloadHistory[key],
                                         temp, key)

                # for p in processes:
                #     p.daemon = True
                #     p.start()

                # for p in processes:
                #     p.join()
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
