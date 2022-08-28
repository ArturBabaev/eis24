from bson.json_util import loads
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['SeriesDB']
collection_accrual = db['accrual']
collection_payment = db['payment']

with open("accrual.bson") as file:
    accruals = loads(file.read())

with open("payment.bson") as file:
    payments = loads(file.read())

if not list(collection_accrual.find()) and not list(collection_payment.find()):
    collection_accrual.insert_many(accruals)
    collection_payment.insert_many(payments)


def debt_search_function(payment, accrual):
    dict_pay = {}

    list_pay = [pay for pay in payment.find().sort('date')]
    list_debt = [debt for debt in accrual.find().sort('date')]

    for pay in list_pay:
        for debt in list_debt:
            if pay['month'] == debt['month'] and pay['date'] == debt['date']:
                dict_pay[pay['_id']] = debt['_id']
                list_pay.remove(pay)
                list_debt.remove(debt)
                break

    for pay in list_pay:
        for debt in list_debt:
            if pay['date'] > debt['date']:
                dict_pay[pay['_id']] = debt['_id']
                if len(list_pay) > 0 and len(list_debt) > 0:
                    list_pay.remove(pay)
                    list_debt.remove(debt)

    list_pay_not_debt = [pay['_id'] for pay in list_pay]

    return dict_pay, list_pay_not_debt


print(debt_search_function(collection_payment, collection_accrual))
