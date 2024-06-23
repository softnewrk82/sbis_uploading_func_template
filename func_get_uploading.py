import requests
import json

import cryptography

import pandas as pd
import numpy as np

import xmltodict

import re
import requests

import warnings
warnings.simplefilter("ignore")

from functools import lru_cache

import importlib

import modules.api_info
importlib.reload(modules.api_info)

from datetime import datetime

from sqlalchemy import create_engine

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encryptvar_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")
var_db_name_for_upl = f_decrypt(var_encryptvar_db_name_for_upl, load_key_external()).decode("utf-8")
var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")


var_day = '01'
var_month = '04'
var_year = '2024'

date_from = "21.06.2024"
date_to = "21.06.2024"


def sbis_real_processing(var_day, var_month, var_year, date_from, date_to):

    var_now = datetime.now()
    # var_day = f"{var_now.day:02d}"
    # var_month = f"{var_now.month:02d}"
    # var_year = f"{var_now.year:02d}"

    # var_day = '01'
    # var_month = '04'
    # var_year = '2024'
    
    var_day = var_day
    var_month = var_month
    var_year = var_year

    # date_from = ".".join([var_day, var_month, var_year])
    # date_to = ".".join([var_day, var_month, var_year])
    # date_from = "21.06.2024"
    # date_to = "21.06.2024"
    
    date_from = date_from
    date_to = date_to

    var_now_B = var_now.strftime("%B")

    print("date_from:", date_from)
    print("date_to:", date_to)
    print("var_now_B:", var_now_B)

    name_unloading = f"sbis_{var_year}{var_month}{var_day}_uploading"
    name_unloading_exc = f"sbis_exc_{var_year}{var_month}{var_day}_uploading"


    def doc_append():
        doc_id.append(var_link)
        doc_type.append(var_doc_type)
        doc_number.append(var_doc_number)
        doc_full_name.append(var_doc_full_name)
        doc_data_main.append(var_doc_data_main)
        doc_at_created.append(var_doc_at_created)
        doc_counterparty_inn.append(var_doc_counterparty_inn)
        doc_counterparty_full_name.append(var_doc_counterparty_full_name)
        doc_provider_inn.append(var_doc_provider_inn)
        doc_provider_full_name.append(var_doc_provider_full_name)

        doc_assigned_manager.append(var_doc_assigned_manager)
        doc_department.append(var_doc_department)

    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)

        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)

    def inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price):
        
        inside_doc_author.append(var_inside_doc_author)
        inside_doc_type.append(var_inside_doc_type)
        inside_doc_item_full_doc_price.append(var_inside_doc_item_full_doc_price)
        
        inside_doc_item_note.append(var_inside_doc_item_note)
        
        inside_doc_item_code.append(var_inside_doc_item_code)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_name.append(var_inside_doc_item_name)
        
        inside_doc_item_quantity.append(var_inside_doc_item_quantity)
        inside_doc_item_unit.append(var_inside_doc_item_unit)
        
        inside_doc_item_price.append(var_inside_doc_item_price)
        inside_doc_item_full_item_price.append(var_inside_doc_item_full_item_price)


    
    def def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________    
            def def_act_vr_print(var_inside_doc_author, var_inside_doc_type):
                
                if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                    print(var_inside_doc_type)
                    print("dict")
                    
                    var_quantity = 1 
                    
                    for x in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["ИнфПолеОписРабот"]:
                        if x["@Идентиф"] == "ПоляНоменклатуры":
                            try:
                                print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                                print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                            except:
                                try:
                                    print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                    print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                except:
                                    print("var_inside_doc_item_article", np.nan)
                        elif x["@Идентиф"] == "Примечание":
                            print("var_inside_doc_item_note", x["@Значен"])            
                        elif x["@Идентиф"] == "НазваниеПоставщика":
                            print("var_inside_doc_item_name", x["@Значен"])            
                        elif x["@Идентиф"] == "КодПоставщика":
                            print("var_inside_doc_item_code", x["@Значен"])    
                                                        
                    try:
                        print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"])
                    except: 
                        pass
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"])
                    try:
                        print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"])
                    except:
                        pass
                    
                    try:
                        print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"])
                    except:
                        print("var_inside_doc_item_price", np.nan) 
                    
                    print("var_inside_doc_item_full_doc_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])     
                    print("_____________________")


                elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                        
                        sum_inside_doc = 0
                        
                        for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                            sum_inside_doc += float(k["@СтоимУчНДС"])
                
                        for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                
                            print(var_inside_doc_type)
                            print("list")
                            
                            var_quantity = 1
                            
                            for x in k["ИнфПолеОписРабот"]:
                                if x["@Идентиф"] == "ПоляНоменклатуры":
                                    try:
                                        print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                                        print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                        var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')  
                                    except:
                                        try:
                                            print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                            print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                            var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                        except:
                                            print("@Артикул", "отсутствует")
                                elif x["@Идентиф"] == "Примечание":
                                    print("var_inside_doc_item_note", x["@Значен"])            
                                elif x["@Идентиф"] == "НазваниеПоставщика":
                                    print("var_inside_doc_item_name", x["@Значен"])            
                                elif x["@Идентиф"] == "КодПоставщика":
                                    print("var_inside_doc_item_code", x["@Значен"])
                                
                                elif x["@Идентиф"] == "Примечание":
                                    var_inside_doc_item_note = x["@Значен"] 
                
                                elif x["@Идентиф"] == "НазваниеПоставщика":
                                    var_inside_doc_item_name = x["@Значен"]
                
                                elif x["@Идентиф"] == "КодПоставщика":  
                                    var_inside_doc_item_code =  x["@Значен"]
                
                            try:
                                print("var_inside_doc_item_quantity", k["@Количество"])
                            except:
                                pass
                            try:
                                print("var_inside_doc_item_name", k["@НаимРабот"])
                            except:
                                pass

                            try:
                                print("var_inside_doc_item_price", k["@Цена"])
                            except:
                                print("var_inside_doc_item_price", np.nan)
                                
                            print("var_inside_doc_item_full_doc_price", k["@СтоимУчНДС"])     
                            print("_____________________")

                        
            # _________________________________________________________________________________________________________________________________________________            
            def def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type):
                if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                    var_quantity = 1  
            
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]
                    
                    var_inside_doc_item_note = ""
                    
                    for x in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["ИнфПолеОписРабот"]:
                        if x["@Идентиф"] == "ПоляНоменклатуры":
                            try:
                                var_inside_doc_item_article = re.findall(f"\d+\-\d+", x["@Значен"])[0]
                                var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                try:
                                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"]
                                except: 
                                    var_inside_doc_item_quantity = var_quantity
                                try:
                                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"]
                                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])
                                except:              
                                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]
                                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])                                            
                            except:
                                try:
                                    var_inside_doc_item_article = re.findall(f"услуга|услуги", x["@Значен"].lower())[0]
                                    var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                    try:
                                        var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"]
                                    except: 
                                        var_inside_doc_item_quantity = var_quantity
                                    
                                    try:
                                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"]
                                        var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])
                                    except:              
                                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]
                                        var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])
                                except:
                                    var_inside_doc_item_article = np.nan
                        elif x["@Идентиф"] == "Примечание":
                            var_inside_doc_item_note = x["@Значен"]
            
                        elif x["@Идентиф"] == "НазваниеПоставщика":
                            var_inside_doc_item_name = x["@Значен"]
            
                        elif x["@Идентиф"] == "КодПоставщика":  
                            var_inside_doc_item_code =  x["@Значен"]
            
                    try:
                        var_inside_doc_item_quantity =  xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"]
                    except: 
                        pass
            
                    try:
                        var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
            
                    try:
                        var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"]
                    except:
                        pass

                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price)
                        
                elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                    
                    sum_inside_doc = 0
                            
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                        sum_inside_doc += float(k["@СтоимУчНДС"])
            
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
            
                        var_quantity = 1
            
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        var_inside_doc_item_full_doc_price = sum_inside_doc
                        var_inside_doc_item_note = ""
            
                        for x in k["ИнфПолеОписРабот"]:
                            if x["@Идентиф"] == "ПоляНоменклатуры":
                                try:
                                    var_inside_doc_item_article = re.findall(f"\d+\-\d+", x["@Значен"])[0]
                                    var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')                                                
                                    try:
                                        var_inside_doc_item_quantity = k["@Количество"]
                                    except: 
                                        var_inside_doc_item_quantity = var_quantity
                                    try:
                                        var_inside_doc_item_price = k["@Цена"]
                                        var_inside_doc_item_full_item_price = float(k["@СтоимУчНДС"])
                                    except:
                                        var_inside_doc_item_price = k["@СтоимУчНДС"]
                                        var_inside_doc_item_full_item_price = float(k["@СтоимУчНДС"])   
                                except:
                                    try:
                                        var_inside_doc_item_article = re.findall(f"услуга|услуги", x["@Значен"].lower())[0]
                                        var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                        var_inside_doc_item_quantity = var_quantity
                                        try:
                                            var_inside_doc_item_quantity = k["@Количество"]
                                        except: 
                                            var_inside_doc_item_quantity = var_quantity
                                
                                        try:
                                            var_inside_doc_item_price = k["@Цена"]
                                            var_inside_doc_item_full_item_price = float(k["@СтоимУчНДС"])
                                        except:
                                            var_inside_doc_item_price = k["@СтоимУчНДС"]
                                            var_inside_doc_item_full_item_price = float(k["@СтоимУчНДС"])
                                    except:
                                        var_inside_doc_item_article = np.nan
                            elif x["@Идентиф"] == "Примечание":
                                var_inside_doc_item_note = x["@Значен"] 
                            elif x["@Идентиф"] == "НазваниеПоставщика":
                                var_inside_doc_item_name = x["@Значен"]
                            elif x["@Идентиф"] == "КодПоставщика":  
                                var_inside_doc_item_code =  x["@Значен"]
            
                        try:
                            var_inside_doc_item_quantity = k["@Количество"]
                        except:
                            pass
                            
                        try:
                            var_inside_doc_item_name = k["@НаимРабот"]
                        except:
                            pass
                        try:
                            var_inside_doc_item_unit = k["@НаимЕдИзм"]
                        except:
                            var_inside_doc_item_unit = np.nan
            
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price)
            # _________________________________________________________________________________________________________________________________________________            

            def_act_vr_print(var_inside_doc_author, var_inside_doc_type)
            def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type)
            
    def def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________    
            def def_edo_nakl_print(var_inside_doc_author, var_inside_doc_type):
                
                if type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == dict:
                    var_inside_doc_item_note = ""
                    print(var_inside_doc_type)
                    print("dict")
                
                    try: 
                        print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", np.nan)
                    print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@КодТов"])
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимТов"])
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НеттоПередано"])
                    try:
                        print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимЕдИзм"])
                    except: 
                        print("var_inside_doc_item_unit", np.nan)
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@Цена"])
                    print("var_inside_doc_item_full_item_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])
                    print("var_inside_doc_item_full_doc_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])
                    print("_____________________")


                elif type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == list:
                    sum_inside_doc = 0
                    
                    for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]:
                        sum_inside_doc += float(k["@СтУчНДС"])
                
                    for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]: 
                        
                        print(var_inside_doc_type)
                        print("list")
                        
                        try:
                            print("var_inside_doc_item_article", k["@АртикулТов"])
                        except:
                            print("var_inside_doc_item_article", "отсутствует")
                        print("var_inside_doc_item_code", k["@КодТов"])
                        print("var_inside_doc_item_name", k["@НаимТов"])
                        print("var_inside_doc_item_quantity", k["@НеттоПередано"])
                        print("var_inside_doc_item_unit", k["@НаимЕдИзм"])
                        print("var_inside_doc_item_price", k["@Цена"])
                        print("var_inside_doc_item_full_item_price", k["@СтУчНДС"])
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                        print("_____________________")
                
                        
            # _________________________________________________________________________________________________________________________________________________            
            def def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type):
                if type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == dict:

                    var_inside_doc_item_note = ""
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"]
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@КодТов"]
                    try:
                        var_inside_doc_item_article = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article = np.nan
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимТов"]
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НеттоПередано"]
                    try:
                        var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@Цена"]
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])

                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price)
                        
                elif type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == list:
                    
                        sum_inside_doc = 0
                        
                    
                        for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]:
                            sum_inside_doc += float(k["@СтУчНДС"])
                        
                        for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]: 
                            
                            var_inside_doc_item_note = ""

                            var_inside_doc_author = var_inside_doc_author
                            var_inside_doc_type = var_inside_doc_type
                            var_inside_doc_item_full_doc_price = sum_inside_doc
                            var_inside_doc_item_code =  k["@КодТов"]
                            try:
                                var_inside_doc_item_article = k["@АртикулТов"]
                            except:
                                var_inside_doc_item_article = np.nan
                                
                            var_inside_doc_item_name = k["@НаимТов"]
                            var_inside_doc_item_quantity =  k["@НеттоПередано"]
                            try:
                                var_inside_doc_item_unit = k["@НаимЕдИзм"]
                            except: 
                                var_inside_doc_item_unit = np.nan
                                
                            var_inside_doc_item_price = k["@Цена"]
                            var_inside_doc_item_full_item_price = float(k["@СтУчНДС"])
                    
            
                            doc_append()
                            inside_doc_append(var_inside_doc_author,
                                var_inside_doc_type,
                                var_inside_doc_item_full_doc_price,
                                var_inside_doc_item_note,
                                var_inside_doc_item_code,
                                var_inside_doc_item_article,
                                var_inside_doc_item_name,
                                var_inside_doc_item_quantity,
                                var_inside_doc_item_unit,
                                var_inside_doc_item_price,
                                var_inside_doc_item_full_item_price)
            # _________________________________________________________________________________________________________________________________________________            

            def_edo_nakl_print(var_inside_doc_author, var_inside_doc_type)
            def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type)

    def def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________    
            def def_act_pp_print(var_inside_doc_author, var_inside_doc_type):
                

                if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                    print(var_inside_doc_type)
                    print("dict")

                    var_inside_doc_item_note = ""
                
                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                
                        try:
                            print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                        except:
                            try:
                                print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
                
                            except:
                                print("var_inside_doc_item_article", np.nan)
                    
                
                
                    print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"])
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"])
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"])
                    try:
                        print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"])
                    except:
                        print("var_inside_doc_item_price", np.nan)               
                    print("var_inside_doc_item_full_item_price", float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"]))
                    sum_inside_doc = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                    print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("_____________________")

                
                elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:
                    print(var_inside_doc_type)
                    print("list")

                    print(type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]))
                    
                    sum_inside_doc = 0
                
                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                        sum_inside_doc += float(k["@Стоим"])
                                                                
                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                        
                        try:
                            print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                        except:
                            try:
                                print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
                
                            except:
                                print("var_inside_doc_item_article", np.nan)
                        
                        print("var_inside_doc_item_code", k["@КодПП"])
                        print("var_inside_doc_item_quantity", k["@КолПП"])
                        print("var_inside_doc_item_name", k["@НаимПП"])
                        try:
                            print("var_inside_doc_item_price", k["@Цена"])
                        except:
                            print("var_inside_doc_item_price", np.nan)
                        print("var_inside_doc_item_full_item_price", float(k["@Стоим"]))
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                        print("_____________________")

                    
                                    
                        
            # _________________________________________________________________________________________________________________________________________________            
            def def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type):


                if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                    
                    var_inside_doc_item_note = ""

                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"]
                
                
                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                
                        try:
                            var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                        except:
                            try:
                                var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
                            except:
                                var_inside_doc_item_article = np.nan
                        
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"]
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"]
                    var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"]
                    except:
                        var_inside_doc_item_price = np.nan
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                    
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price) 

                
                elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:      

                    sum_inside_doc = 0

                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                        sum_inside_doc += float(k["@Стоим"])             
                    
                    for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:

                        var_inside_doc_item_note = ""
            
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        var_inside_doc_item_full_doc_price = sum_inside_doc
                        var_inside_doc_item_code = k["@КодПП"]
            
                        try:
                            var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                        except:
                            try:
                                var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
            
                            except:
                                var_inside_doc_item_article = np.nan
            
            
                            
                        var_inside_doc_item_name = k["@НаимПП"]
                        try:
                            var_inside_doc_item_quantity = k["@КолПП"]
                        except:
                            var_inside_doc_item_quantity = np.nan

                        var_inside_doc_item_unit = np.nan
                        try:
                            var_inside_doc_item_price = k["@Цена"]
                        except:
                            var_inside_doc_item_price = np.nan
                            
                        var_inside_doc_item_full_item_price = float(k["@Стоим"])       
                        
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price) 
        
            # _________________________________________________________________________________________________________________________________________________            

            def_act_pp_print(var_inside_doc_author, var_inside_doc_type)
            def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type)

    def def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________    
            def def_upd_dop_print(var_inside_doc_author, var_inside_doc_type):

                if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                
                    print(var_inside_doc_type)
                    print("dict")
                
                    var_inside_doc_item_note = ""
                    
                    try:
                        print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                    except:
                        print("var_inside_doc_item_quantity", np.nan)
                
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                    try:
                        print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                    except: 
                        print("var_inside_doc_item_price", np.nan)
                    try:
                        print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                    except: 
                        print("var_inside_doc_item_article", np.nan)
                
                    print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                    try:
                        print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
                
                    try:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))


                elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                    
                    print(var_inside_doc_type)
                    print("list")
                
                    
                    sum_inside_doc = 0
                                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                            
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
                        
                        print("list")
                        
                        try:                
                            print("var_inside_doc_item_quantity", k["@КолТов"])
                        except:
                            print("var_inside_doc_item_quantity", np.nan)
                            
                        print("var_inside_doc_item_name", k["@НаимТов"])
                        try:
                            print("var_inside_doc_item_price", k["@ЦенаТов"])
                        except:
                            print("var_inside_doc_item_price", np.nan)
                        try:
                            print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                        except:
                            print("var_inside_doc_item_article", np.nan)
                
                        print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                        try:
                            print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                        except:
                            print("var_inside_doc_item_unit", np.nan)
                        try:
                            print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                        except: 
                            print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                        print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
            # _________________________________________________________________________________________________________________________________________________            

            def def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type):
                
                if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:

                    var_inside_doc_item_note = ""
                    
                    sum_inside_doc = 0
                    sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
                
                
                    try:
                        var_inside_doc_item_article =  xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                    except: 
                        var_inside_doc_item_article = np.nan
                        
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                    try:
                        var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                    except: 
                        var_inside_doc_item_quantity = np.nan
                    try:
                        var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                    except: 
                        var_inside_doc_item_price = np.nan
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
        
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price)  

                elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                
                    sum_inside_doc = 0
                                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                            
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                     
                    
                    
                        var_inside_doc_item_note = ""
                    
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        var_inside_doc_item_full_doc_price = sum_inside_doc
                        var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
                        try:
                            var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                        except:
                            var_inside_doc_item_article =  np.nan
                            
                        var_inside_doc_item_name = k["@НаимТов"]
                        
                        try:
                            var_inside_doc_item_quantity = k["@КолТов"]
                        except:
                            var_inside_doc_item_quantity = np.nan
                
                        try:
                            var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                        except:
                            var_inside_doc_item_unit = np.nan
                        try:
                            var_inside_doc_item_price = k["@ЦенаТов"]
                        except:
                            var_inside_doc_item_price = np.nan
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                    
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price)    
            # _________________________________________________________________________________________________________________________________________________            

            def_upd_dop_print(var_inside_doc_author, var_inside_doc_type)
            def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type)

    def def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________    
            def def_upd_s_dop_print(var_inside_doc_author, var_inside_doc_type):

                if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                    
                    print(var_inside_doc_type)
                    print("dict")

                    try:
                        print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                    except: 
                        print("var_inside_doc_item_quantity", np.nan)
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                    
                    try:
                        print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    
                    try:
                        print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", np.nan)
                    
                    print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                    
                    try:
                        print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
                
                    try:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))
                    
                    sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                    print("var_inside_doc_item_full_item_price", sum_inside_doc)


                elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                    
                    print(var_inside_doc_type)
                    print("list")
                                    
                    sum_inside_doc = 0
                                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                                                                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
                
                        print("list")
                        try:
                            print("var_inside_doc_item_quantity",k["@КолТов"])
                        except: 
                            print("var_inside_doc_item_quantity",np.nan)
                        print("var_inside_doc_item_name", k["@НаимТов"])
                        try:
                            print("var_inside_doc_item_price", k["@ЦенаТов"])
                        except:
                            print("var_inside_doc_item_price", np.nan)
                        try:
                            print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                        except: 
                            print("var_inside_doc_item_article", np.nan)
                        print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                        try:
                            print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                        except:
                            print("var_inside_doc_item_unit", np.nan)
                
                        try: 
                            print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                        except: 
                            print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                        print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
                            
            # _________________________________________________________________________________________________________________________________________________            

            def def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type):
                
                if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                    
                    var_inside_doc_item_note = ""


                
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
                
                    try:
                        var_inside_doc_item_article = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article = np.nan
                                            
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                    try:
                        var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                    except: 
                        var_inside_doc_item_unit = np.nan
                
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                    
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                    
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price)    

                elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:

                    sum_inside_doc = 0    
                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                                                                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:  
                    
                        var_inside_doc_item_note = ""
                
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        var_inside_doc_item_full_doc_price = sum_inside_doc
                        var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
                
                
                        try:
                            var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                        except:
                            var_inside_doc_item_article =  np.nan
                
                            
                        var_inside_doc_item_name = k["@НаимТов"]
                        var_inside_doc_item_quantity = k["@КолТов"]
                        try:
                            var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                        except:
                            var_inside_doc_item_unit = np.nan
                                
                        var_inside_doc_item_price = k["@ЦенаТов"]
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])        
        
                    
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price)      
            # _________________________________________________________________________________________________________________________________________________            

            def_upd_s_dop_print(var_inside_doc_author, var_inside_doc_type)
            def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type)
            
            
    url = url_sbis

    method = "СБИС.Аутентифицировать"
    params = {
        "Параметр": {
            "Логин": API_sbis,
            "Пароль": API_sbis_pass
        }

    }
    parameters = {
    "jsonrpc": "2.0",
    "method": method,
    "params": params,
    "id": 0
    }

    response = requests.post(url, json=parameters)
    response.encoding = 'utf-8'

    str_to_dict = json.loads(response.text)
    access_token = str_to_dict["result"]
    print("access_token:", access_token)

    headers = {
    "X-SBISSessionID": access_token,
    "Content-Type": "application/json",
    }  

    # _____________________________________________________________
    doc_id = []
    doc_type = []
    doc_number = []
    doc_full_name = []
    doc_data_main = []
    doc_at_created = []

    doc_counterparty_inn = []
    doc_counterparty_full_name = []

    doc_provider_inn = []
    doc_provider_full_name = []

    doc_assigned_manager = []
    doc_department = []

    inside_doc_author = []
    inside_doc_type = []
    inside_doc_item_full_doc_price = []

    inside_doc_item_note = []

    inside_doc_item_code = []
    inside_doc_item_article = []
    inside_doc_item_name = []

    inside_doc_item_quantity = []
    inside_doc_item_unit = []

    inside_doc_item_price = []
    inside_doc_item_full_item_price = []
    # ___________________________________________________________________________________________
    doc_id_exc = []
    doc_type_exc = []
    doc_number_exc = []
    doc_full_name_exc = []
    doc_data_main_exc = []
    doc_at_created_exc = []

    doc_counterparty_inn_exc = []
    doc_counterparty_full_name_exc = []

    doc_provider_inn_exc = []
    doc_provider_full_name_exc = []

    doc_assigned_manager_exc = []
    doc_department_exc = []
    # ___________________________________________________________________________________________

    var_status_has_more = "Да"
    i_page = 0

    while var_status_has_more == "Да":
        
        parameters_real = {
        "jsonrpc": "2.0",
        "method": "СБИС.СписокДокументов",
        "params": {
            "Фильтр": {
            "ДатаС": date_from,
            "ДатаПо": date_to,
            "Тип": "ДокОтгрИсх",
            "Регламент": {
                "Название": "Реализация"
            },
            "Навигация": {
                "Страница": i_page
            }
            }
        },
        "id": 0
        }
        
        url_real = url_sbis_unloading
        
        response_points = requests.post(url_real, json=parameters_real, headers=headers)
        str_to_dict_points_main = json.loads(response_points.text)
        
        json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()
        
        with open("DICT_REALIZE.json", 'w') as json_file_points_o:
            json_file_points_o.write(json_data_points)
        
        j = 0
        for i in str_to_dict_points_main["result"]["Документ"]:
            print(j)
            j += 1
            if (re.findall("реал", i["Регламент"]["Название"].lower())[-1] == "реал") and (i["Расширение"]["Проведен"].lower() == 'да'):
    # ___________________________________________________________________________________________
                var_link = i["Идентификатор"]
                doc_manager_name = " ".join([str(i["Ответственный"]["Фамилия"]), str(i["Ответственный"]["Имя"]), str(i["Ответственный"]["Отчество"])])

                var_doc_type = i["Регламент"]["Название"]
                var_doc_number = i["Номер"] 
                var_doc_full_name = i["Название"]
                try:
                    var_doc_data_main = i["Дата"]
                except:
                    var_doc_data_main = np.nan
                var_doc_at_created = i["ДатаВремяСоздания"]
                try:
                    var_doc_counterparty_inn = i["Контрагент"]["СвФЛ"]["ИНН"]
                    var_doc_counterparty_full_name = i["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                except:
                    var_doc_counterparty_inn = i["Контрагент"]["СвЮЛ"]["ИНН"]
                    var_doc_counterparty_full_name = i["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                try:
                    var_doc_provider_inn = i["НашаОрганизация"]["СвФЛ"]["ИНН"]
                    var_doc_provider_full_name = i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                except:
                    var_doc_provider_inn = i["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                    var_doc_provider_full_name = i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                var_doc_assigned_manager = doc_manager_name
                try:
                    var_doc_department = i["Подразделение"]["Название"]
                except:
                    var_doc_department = np.nan    
    # ___________________________________________________________________________________________
                print("var_link", var_link)
                print("var_doc_type", i["Регламент"]["Название"])

                print("var_doc_number", i["Номер"])
                print("var_doc_full_name", i["Название"])
                try:
                    print("var_doc_data_main", i["Дата"])
                except:
                    print("var_doc_data_main", np.nan)
                print("var_doc_at_created", i["ДатаВремяСоздания"])
                try:
                    print("var_doc_counterparty_inn", i["Контрагент"]["СвФЛ"]["ИНН"])
                    print("var_doc_counterparty_full_name", i["Контрагент"]["СвФЛ"]["НазваниеПолное"])
                except:
                    print("var_doc_counterparty_inn", i["Контрагент"]["СвЮЛ"]["ИНН"])
                    print("var_doc_counterparty_full_name", i["Контрагент"]["СвЮЛ"]["НазваниеПолное"])           
                try:
                    print("var_doc_provider_inn", i["НашаОрганизация"]["СвФЛ"]["ИНН"])
                    print("var_doc_provider_full_name", i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"])
                except:
                    print("var_doc_provider_inn", i["НашаОрганизация"]["СвЮЛ"]["ИНН"])
                    print("var_doc_provider_full_name", i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"])
            
                print("var_doc_assigned_manager", doc_manager_name)
                try:
                    print("var_doc_department", i["Подразделение"]["Название"])
                except:
                    print("var_doc_department", np.nan)
    # ___________________________________________________________________________________________

                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ПрочитатьДокумент",
                "params": {
                    "Документ": {
                        "Идентификатор": var_link,
                        "ДопПоля": "ДополнительныеПоля"
                    }
                },
                "id": 0
                }
            
                url_real = url_sbis_unloading
            
                response_points = requests.post(url_real, json=parameters_real, headers=headers)
                str_to_dict_points = json.loads(response_points.text)
    # ___________________________________________________________________________________________
                author_list = [str_to_dict_points["result"]["Автор"]["Имя"], str_to_dict_points["result"]["Автор"]["Фамилия"], str_to_dict_points["result"]["Автор"]["Отчество"]]
                print("автор:", " ".join(author_list))
                var_inside_doc_author = " ".join(author_list)
    # ___________________________________________________________________________________________
                attachments_id = {}
                try:
                    for i in range(len(str_to_dict_points["result"]["ВложениеУчета"])):
                        if str_to_dict_points["result"]["ВложениеУчета"][i]["Тип"].lower() in ("актпп", "актвр", "эдонакл", "упддоп", "упдсчфдоп"):
                            print(str_to_dict_points["result"]["ВложениеУчета"][i]["Тип"].lower())
                            attachments_id[str_to_dict_points["result"]["ВложениеУчета"][i]["Тип"].lower()] = str_to_dict_points["result"]["ВложениеУчета"][i]["Файл"]["Ссылка"]
                        else:
                            pass
                except:
                    pass
    # ___________________________________________________________________________________________           
                if len(attachments_id) == 0:
                    
                    doc_append_exc()               
    # ___________________________________________________________________________________________            
                elif len(attachments_id) > 0:
                
                    
                    
                    for b in attachments_id.keys():
                        
                        if  b == "актвр":
                                        
                            # ___________________________________________________________________________________________    
                            a = requests.get(attachments_id["актвр"], headers=headers)
                            a.encoding = "cp1251"
                            xml_a = xmltodict.parse(a.text)
                            
                            # ___________________________________________________________________________________________    
                            var_inside_doc_type = "актвр"
                            # ___________________________________________________________________________________________    


                            def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type)
                        
                        # _______________________________________________________________________________________________________________________________
                            
                        elif  b == "эдонакл":
                    
                            # ___________________________________________________________________________________________    
                            a = requests.get(attachments_id["эдонакл"], headers=headers)
                            a.encoding = "cp1251"
                            xml_a = xmltodict.parse(a.text)
                    
                            # _______________________________________________________________________________________________________________________________
                            var_inside_doc_type = "эдонакл"
                            # _______________________________________________________________________________________________________________________________

                            def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type)
                        
                        # _______________________________________________________________________________________________________________________________
                        
                        elif  b == "актпп":     
                                
                            # _______________________________________________________________________________________________________________________________
                            a = requests.get(attachments_id["актпп"], headers=headers)
                            a.encoding = "cp1251"
                            xml_a = xmltodict.parse(a.text)    
                            
                            # _______________________________________________________________________________________________________________________________
                            var_inside_doc_type = "актпп"
                            # _______________________________________________________________________________________________________________________________
                    
                            def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type)
                                    
                        
                        # _______________________________________________________________________________________________________________________________
                        
                        elif  b == "упддоп":   
                            
                                # _______________________________________________________________________________________________________________________________
                                a = requests.get(attachments_id["упддоп"], headers=headers)
                                a.encoding = "cp1251"
                                xml_a = xmltodict.parse(a.text)    
                            
                                # _______________________________________________________________________________________________________________________________
                                var_inside_doc_type = "упддоп"         
                                # _______________________________________________________________________________________________________________________________
                            
                                def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type)

                        # _______________________________________________________________________________________________________________________________
                        elif  b == "упдсчфдоп":     
                                a = requests.get(attachments_id["упдсчфдоп"], headers=headers)
                                a.encoding = "cp1251"
                                xml_a = xmltodict.parse(a.text)    
                                
                                # _______________________________________________________________________________________________________________________________
                                var_inside_doc_type = "упдсчфдоп"
                                # _______________________________________________________________________________________________________________________________
        
                                def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type)
                                    
                else:
                    doc_append_exc()      
                                        
        if var_status_has_more == "Нет":
            break
        elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
            i_page += 1
        else:
            pass
        var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
        print("ЕстьЕще", var_status_has_more)
        print("___________________________________________________________________________________________________________________________________________________________")
        print(f"СЛЕДУЮЩАЯ СТРАНИЦА {i_page}")       
            
            
    lst_append = [doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,

    doc_counterparty_inn,
    doc_counterparty_full_name,

    doc_provider_inn,
    doc_provider_full_name,

    doc_assigned_manager,
    doc_department,

    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price,

    inside_doc_item_note,

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,

    inside_doc_item_quantity,
    inside_doc_item_unit,

    inside_doc_item_price,
    inside_doc_item_full_item_price,
    ]

    lst_append_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",
        
        "inside_doc_author",
        "inside_doc_type",
        "inside_doc_item_full_doc_price",
        
        "inside_doc_item_note",
        
        "inside_doc_item_code",
        "inside_doc_item_article",
        "inside_doc_item_name",
        
        "inside_doc_item_quantity",
        "inside_doc_item_unit",
        
        "inside_doc_item_price",
        "inside_doc_item_full_item_price",
    ]    
            
    lst_append_exc_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",
    ]     
            
    df = pd.DataFrame(columns=lst_append_name, data=list(zip(
    doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,

    doc_counterparty_inn,
    doc_counterparty_full_name,

    doc_provider_inn,
    doc_provider_full_name,

    doc_assigned_manager,
    doc_department,

    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price,

    inside_doc_item_note,

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,

    inside_doc_item_quantity,
    inside_doc_item_unit,

    inside_doc_item_price,
    inside_doc_item_full_item_price    
    )))
        
    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)

        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)       
            
    df_exc = pd.DataFrame(columns=lst_append_exc_name, data=list(zip(
    doc_id_exc,
    doc_type_exc,
    doc_number_exc,
    doc_full_name_exc,
    doc_data_main_exc,
    doc_at_created_exc,

    doc_counterparty_inn_exc,
    doc_counterparty_full_name_exc,

    doc_provider_inn_exc,
    doc_provider_full_name_exc,

    doc_assigned_manager_exc,
    doc_department_exc,
    )))
        
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df.to_sql(name=f'{name_unloading}', con=my_conn, if_exists="replace")
        print("df.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()')        
            
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df_exc.to_sql(name=f'{name_unloading_exc}', con=my_conn, if_exists="replace")
        print("df_exc.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()') 


sbis_real_processing(var_day, var_month, var_year, date_from, date_to)
