from pymongo import MongoClient
import random

class DataService(object):

    @classmethod
    def init(cls, client):
        cls.client = client
        cls.db = client.appstore
        cls.userDownloadHistory = cls.db.user_download_history
        cls.appInfo = cls.db.app_info

    @classmethod
    def retrieveUserDownloadHistory(cls, filterDict = {}):
        # get the userid and their download history in a dict
        result = {}
        cursor = cls.userDownloadHistory.find(filterDict)
        for history in cursor:
            result[history["user_id"]] = history["download_history"]
        return result

    @classmethod
    def retrieveAppInfo(cls, filterDict = {}):
        # get all app id and name
        result = {}
        cursor = cls.appInfo.find(filterDict)
        for app in cursor:
            result[app["app_id"]] = {'title ': app["title"]}
        return result

    @classmethod
    def updateUserInfo(cls, filterDict, update):
        cls.userDownloadHistory.update_one(filterDict, update, True)

    @classmethod
    def updateAppInfo(cls, filterDict, update):
        cls.appInfo.update_one(filterDict, update, True)
