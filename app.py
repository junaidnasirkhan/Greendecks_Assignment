from flask import Flask,request
from flask import jsonify, make_response
import json
import os
import gdown
from itertools import chain, starmap
from flask_cors import CORS
app = Flask(__name__)
dataset=[] # to hold the processed data

class RequestHandler(object):
    """Class to handle JSON request,Process it and generate response"""
    
    def __init__(self):
        """Initialize all index variable"""
        
        self.query_type = ["discounted_products_list",
                          "discounted_products_count",
                          "avg_discount",
                          "discounted_products_count|avg_discount",
                          "expensive_list",
                          "competition_discount_diff_list"]

        self.NAP = {'id':'_id_$oid',
                   'brand':'brand_name',
                   'regular_price':'price_regular_price_value',
                   'offer_price':'price_offer_price_value',
                   'basket_price':'price_basket_price_value',
                   'is_competetor':'similar_products_meta_total_results'}

        self.competitors = ['5da94f4e6d97010001f81d72',
                           '5da94f270ffeca000172b12e',
                           '5d0cc7b68a66a100014acdb0',
                           '5da94ef80ffeca000172b12c',
                           '5da94e940ffeca000172b12a']

        self.WHICH_COMPETETOR = {'5d0cc7b68a66a100014acdb0':'similar_products_website_results_5d0cc7b68a66a100014acdb0_meta_total_results',
                                '5da94e940ffeca000172b12a':'similar_products_website_results_5da94e940ffeca000172b12a_meta_total_results',
                                '5da94ef80ffeca000172b12c':'similar_products_website_results_5da94ef80ffeca000172b12c_meta_total_results',
                                '5da94f270ffeca000172b12e':'similar_products_website_results_5da94f270ffeca000172b12e_meta_total_results',
                                '5da94f4e6d97010001f81d72':'similar_products_website_results_5da94f4e6d97010001f81d72_meta_total_results'}

        self.COM_OFFER_PRICE = {'5d0cc7b68a66a100014acdb0':'similar_products_website_results_5d0cc7b68a66a100014acdb0_knn_items_0__source_price_offer_price_value',
                               '5da94e940ffeca000172b12a':'similar_products_website_results_5da94e940ffeca000172b12a_knn_items_0__source_price_offer_price_value',
                               '5da94ef80ffeca000172b12c':'similar_products_website_results_5da94ef80ffeca000172b12c_knn_items_0__source_price_offer_price_value',
                               '5da94f270ffeca000172b12e':'similar_products_website_results_5da94f270ffeca000172b12e_knn_items_0__source_price_offer_price_value',
                               '5da94f4e6d97010001f81d72':'similar_products_website_results_5da94f4e6d97010001f81d72_knn_items_0__source_price_offer_price_value'}

        self.COM_REGULAR_PRICE = {'5d0cc7b68a66a100014acdb0':'similar_products_website_results_5d0cc7b68a66a100014acdb0_knn_items_0__source_price_regular_price_value',
                                '5da94e940ffeca000172b12a':'similar_products_website_results_5da94e940ffeca000172b12a_knn_items_0__source_price_regular_price_value',
                                '5da94ef80ffeca000172b12c':'similar_products_website_results_5da94ef80ffeca000172b12c_knn_items_0__source_price_regular_price_value',
                                '5da94f270ffeca000172b12e':'similar_products_website_results_5da94f270ffeca000172b12e_knn_items_0__source_price_regular_price_value',
                                '5da94f4e6d97010001f81d72':'similar_products_website_results_5da94f4e6d97010001f81d72_knn_items_0__source_price_regular_price_value'}

        self.COM_BASKET_PRICE = {'5d0cc7b68a66a100014acdb0':'similar_products_website_results_5d0cc7b68a66a100014acdb0_knn_items_0__source_price_basket_price_value',
                                '5da94e940ffeca000172b12a':'similar_products_website_results_5da94e940ffeca000172b12a_knn_items_0__source_price_basket_price_value',
                                '5da94ef80ffeca000172b12c':'similar_products_website_results_5da94ef80ffeca000172b12c_knn_items_0__source_price_basket_price_value',
                                '5da94f270ffeca000172b12e':'similar_products_website_results_5da94f270ffeca000172b12e_knn_items_0__source_price_basket_price_value',
                                '5da94f4e6d97010001f81d72':'similar_products_website_results_5da94f4e6d97010001f81d72_knn_items_0__source_price_basket_price_value'}

        self.essential_keys = list(self.NAP.values()) + \
                             list(self.WHICH_COMPETETOR.values()) + \
                             list(self.COM_REGULAR_PRICE.values()) + \
                             list(self.COM_OFFER_PRICE.values()) + \
                             list(self.COM_BASKET_PRICE.values())

    
################################################### Preprocessing Functions ###############################################

# reference:- https://stackoverflow.com/questions/55504380/flatten-nested-dictionary-with-dictionary-embedded-in-lists-functional-python
    def flatten_json(self,dictionary):
        """Flatten a nested json file"""

        def unpack(parent_key, parent_value):
            """Unpack one level of nesting in json file"""
            # Unpack one level only!!!

            if isinstance(parent_value, dict):
                for key, value in parent_value.items():
                    temp1 = parent_key + '_' + key
                    yield temp1, value
            elif isinstance(parent_value, list):
                i = 0 
                for value in parent_value:
                    temp2 = parent_key + '_'+str(i) 
                    i += 1
                    yield temp2, value
            else:
                yield parent_key, parent_value    


        # Keep iterating until the termination condition is satisfied
        while True:
            # Keep unpacking the json file until all values are atomic elements (not dictionary or list)
            dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
            # Terminate condition: not any value in the json file is dictionary or list
            if not any(isinstance(value, dict) for value in dictionary.values()) and \
               not any(isinstance(value, list) for value in dictionary.values()):
                break

        return dictionary

    def remove_unwanted(self,dictionary,essential_keys):
        """Removes all unwanted data from the flatten dictonary"""
        
        keys = list(dictionary.keys())
        for i in keys:
            if i not in self.essential_keys:
                del dictionary[i]
        return dictionary


####################################################### END ###############################################################
    


################################################### JSON request query verification Functions ##############################


    def verify_query(self,query):
        """Verifies the JSON request query, whether it has accepted format or not"""
        
        flag = 0
        if query.get("query_type") in self.query_type:
            if (query.get("query_type") == "competition_discount_diff_list"):

                if (query.get("filters") is not None):
                    for i in query.get("filters"):
                        if i["operand1"] == "discount_diff":
                            flag+=1
                        if i["operand1"] == "competition":
                            flag+=1
                    if flag == 2:
                        return self.check_filter(query.get("filters"))
                    else:
                        return False
                else:
                    return False


            if query.get("filters") is not None:
                return self.check_filter(query.get("filters"))
            else:
                return True
        else:
            return False

    def check_filter(self,filters):
        """Check the filters in a JSON request query, whether it has accepted format or not"""
        
        flag = True
        for i in filters:
            if i["operand1"] == "discount":
                if i["operator"] in [">","<","=="]:
                    if type(i["operand2"]) in [int,float]:
                        continue
                    else:
                        flag=False
                        break

                else:
                    flag=False
                    break

            elif i["operand1"] == "brand.name":
                if i["operator"] == "==":
                    if type(i["operand2"]) == str:
                        continue 
                    else:
                        flag=False
                        break

                else:
                    flag=False
                    break
            elif i["operand1"] == "competition":
                if i["operator"] == "==":
                    if type(i["operand2"]) == str:
                        continue 
                    else:
                        flag=False
                        break
                else:
                    flag=False
                    break

            elif i["operand1"] == "discount_diff":
                if i["operator"] ==">":
                    if type(i["operand2"]) in [int,float]:
                        continue
                    else:
                        flag=False
                        break
                else:
                    flag = False
                    break
            else:
                flag=False
                break
        return flag
    
    
####################################################### END ###############################################################



######################################################## Utility Functions ################################################


    def get_discount(self,regular,offer):
        """Calculates discount % given regular price and offer price""" 
        
        return ((regular-offer)/regular) * 100

    def get_id(self,dataset):
        """Returns the product id given a dataset"""
        
        _id = []
        for i in dataset:
            _id.append(i[self.NAP['id']])
        return _id
    
    
    
####################################################### END ###############################################################



########################################################## To Apply Filters ###############################################
    def filter(self,operand_1,operator,operand_2,dataset):
        """Apply filters and return index of filtered products"""
        
        filtered = []
        if operand_1 == "discount":
            if operator == ">":
                for i in range(len(dataset)):
                    temp = dataset[i]
                    if self.get_discount(temp[self.NAP['regular_price']],temp[self.NAP['offer_price']]) > operand_2:
                        filtered.append(i)

            elif operator == "<":
                for i in range(len(dataset)):
                    temp = dataset[i]
                    if self.get_discount(temp[self.NAP['regular_price']],temp[self.NAP['offer_price']]) < operand_2:
                        filtered.append(i)
            else:
                for i in range(len(dataset)):
                    temp = dataset[i]
                    if self.get_discount(temp[self.NAP['regular_price']],temp[self.NAP['offer_price']]) == operand_2:
                        filtered.append(i)

        elif operand_1 == "brand.name":
            for i in range(len(dataset)):
                temp=dataset[i]
                if temp[self.NAP['brand']] == operand_2:
                    filtered.append(i)

        elif operand_1 == "competition":    
            for i in range(len(dataset)):
                temp=dataset[i]
                if self.NAP['is_competetor'] in temp.keys():
                    if temp[self.NAP['is_competetor']] > 0:
                        if temp[self.WHICH_COMPETETOR[operand_2]] > 0:
                            filtered.append(i)

        elif operand_1 == "discount_diff":
            for i in range(len(dataset)):
                temp=dataset[i]
                if self.NAP['is_competetor'] in temp.keys():
                    if temp[self.NAP['is_competetor']] > 0:
                        for j in self.competitors:
                            if temp[self.WHICH_COMPETETOR[j]] > 0:
                                nap_basket = temp[self.NAP["basket_price"]]
                                com_basket = temp[self.COM_BASKET_PRICE[j]]
                                if nap_basket > com_basket:
                                    diff = nap_basket - com_basket
                                    avg = (nap_basket + com_basket)/2
                                    per_diff = (diff/avg) * 100
                                    if per_diff > operand_2:
                                        filtered.append(i)


        return filtered
    
####################################################### END ###############################################################



################################################# Query type handler functions ############################################
    def discounted_products_list(self,dataset):
        """Get the discounted products and return index of discounted products"""
        
        discounted = []
        for i in range(len(dataset)):
            temp = dataset[i]
            if self.get_discount(temp[self.NAP['regular_price']],temp[self.NAP['offer_price']]) > 0:
                discounted.append(i)
        return discounted

    def discounted_products_count(self,dataset):
        """Count discounted products"""
        
        return len(dataset)

    def discounted_products_count_avg_discount(self,dataset):
        """Count dicounted products and Calculate average discount %"""
        
        count = self.discounted_products_count(dataset)
        avg_discount = self.get_avg_discount(dataset)
        return count,avg_discount


    def competition_discount_diff_list(self,dataset,filters):
        """Calculate discount difference % with the competetor"""
        
        filtered = []
        for i in filters:
            if i["operand1"] == "discount_diff":
                value = i["operand2"]
            if i["operand1"] == "competition":
                competetor = i["operand2"]
        for i in range(len(dataset)):
            temp=dataset[i]
            if self.NAP['is_competetor'] in temp.keys():
                if temp[self.NAP['is_competetor']] > 0:
                    if temp[self.WHICH_COMPETETOR[competetor]] > 0:
                        nap_basket = temp[self.NAP["basket_price"]]
                        com_basket = temp[self.COM_BASKET_PRICE[competetor]]
                        if nap_basket > com_basket:
                            diff = nap_basket - com_basket
                            avg = (nap_basket + com_basket)/2
                            per_diff = (diff/avg) * 100
                            if per_diff > value:
                                filtered.append(i)
        return filtered
    
    def get_avg_discount(self,dataset):
        """Calculates the average discount"""
        
        discount = []
        for i in dataset:
            discount.append(self.get_discount(i[self.NAP['regular_price']],i[self.NAP['offer_price']]))
        if len(discount)>0:
            return sum(discount)/len(discount)
        else:
            return 0

    def expensive_list(self,dataset):
        """Returns the index of expensive products by comparing with the basket price of competetors"""
        
        expensive = []
        for i in range(len(dataset)):
            temp = dataset[i]
            if self.NAP['is_competetor'] in temp.keys():
                if temp[self.NAP['is_competetor']] > 0:
                    for j in self.competitors:
                        if self.COM_BASKET_PRICE[j] in temp.keys():
                            if temp[self.NAP['basket_price']]>temp[self.COM_BASKET_PRICE[j]]:
                                expensive.append(i)
                                break
        return expensive
    
    
####################################################### END ###############################################################
    

    
#################################################### Query processing functions ###########################################

    def refine(self,index,temp):
        """Refines the dataset given index"""
        
        return [temp[i] for i in index]

    def process_query(self,query):
        """Process the JSON query request according to the query_type and generate the JSON respose accordingly """
        
        if self.verify_query(query):
            if query.get("query_type") == "discounted_products_list":
                index = self.discounted_products_list(dataset)
                temp = self.refine(index,dataset)
                if query.get("filters") is not None:
                    for i in query.get("filters"):
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = { "discounted_products_list": self.get_id(temp) if len(self.get_id(temp)) > 0 else "No record found !" }

            elif query.get("query_type") == "discounted_products_count":
                index = self.discounted_products_list(dataset)
                temp = self.refine(index,dataset)
                if query.get("filters") is not None:
                    for i in query.get("filters"):
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = {"discounted_products_count" : len(temp)}

            elif query.get("query_type") == "avg_discount":
                index = self.discounted_products_list(dataset)
                temp = self.refine(index,dataset)
                if query.get("filters") is not None:
                    for i in query.get("filters"):
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = {"avg_discount": self.get_avg_discount(temp)}

            elif query.get("query_type") == "discounted_products_count|avg_discount":
                index = self.discounted_products_list(dataset)
                temp = self.refine(index,dataset)
                if query.get("filters") is not None:
                    for i in query.get("filters"):
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = { "discounted_products_count": len(temp), "avg_dicount": self.get_avg_discount(temp) }

            elif query.get("query_type") == "expensive_list":
                index = self.expensive_list(dataset)
                temp = self.refine(index,dataset)
                if query.get("filters") is not None:
                    for i in query.get("filters"):
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = { "expensive_list": self.get_id(temp) if len(self.get_id(temp)) > 0 else "No record found !" }

            elif query.get("query_type") == "competition_discount_diff_list":
                index = self.competition_discount_diff_list(dataset,query.get("filters"))
                temp = self.refine(index,dataset)
                for i in query.get("filters"):
                    if (i["operand1"] == "discount_diff") or (i["operand1"] == "competition"):
                        continue
                    else:
                        index = self.filter(i['operand1'],i['operator'],i['operand2'],temp)
                        temp = self.refine(index,temp)
                response = { "competition_discount_diff_list": self.get_id(temp) if len(self.get_id(temp)) > 0 else "No record found !" }

        else:
            response = {"Error" : "Unsupported request! Please verify query_type, filter operands & filter operators"}

        return response
    
####################################################### END ###############################################################


@app.before_first_request
def init_files(dump_path = 'dumps/netaporter_gb.json'):
    """It executes once before first request, Downloads & Preprocess the dataset """
    
    url = 'https://drive.google.com/uc?id=1WIXzwvk2GI0BZjxFYihd3rNu-OA6TWQO&export=download'
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    if os.path.exists(dump_path):
        pass
    else:
        gdown.download(url = url, output = dump_path, quiet=False)
    

    global dataset
    dataset = []
    req_handler = RequestHandler()
    with open(dump_path, mode='rb') as fp:
        for product in fp.readlines():
            dictionary = req_handler.remove_unwanted(req_handler.flatten_json(json.loads(product)), req_handler.essential_keys)
            dataset.append(dictionary)





@app.route("/", methods=["POST","GET"])
def request_handler():
    """Handles the POST and GET requests appropriately"""

    if request.is_json:
        req_handler = RequestHandler()
        request_body = request.get_json()
        response_body = req_handler.process_query(request_body)
        return make_response(jsonify(response_body,200))
    else:

        return "<h1 align='center'>The API Devloped for Green Deck's Data Science Assignment.</h1>",200

if __name__ == "__main__":
    
    app.run(threaded = True)