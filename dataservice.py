# This file create a DataService class, used to get and update the information
# in MongoDB

from pymongo import MongoClient
import random

class DataService(object):

    # initialize the class
    @classmethod
    def init(cls, client):
        cls.client = client
        cls.db = client.appstore
        cls.userDownloadHistory = cls.db.user_download_history
        cls.appInfo = cls.db.app_info

    # get user download history from the collection in MongoDB
    @classmethod
    def retrieveUserDownloadHistory(cls, filterDict = {}):
        # get the userid and their download history in a dict
        result = {}
        cursor = cls.userDownloadHistory.find(filterDict)
        for history in cursor:
            result[history["user_id"]] = history["download_history"]
        return result

    # get app info from the collection in MongoDB
    @classmethod
    def retrieveAppInfo(cls, filterDict = {}):
        # get all app id and name
        result = {}
        cursor = cls.appInfo.find(filterDict)
        for app in cursor:
            result[app["app_id"]] = {'title ': app["title"]}
        return result

    # update users info
    @classmethod
    def updateUserInfo(cls, filterDict, update):
        cls.userDownloadHistory.update_one(filterDict, update, True)

    # update apps info
    @classmethod
    def updateAppInfo(cls, filterDict, update):
        cls.appInfo.update_one(filterDict, update, True)