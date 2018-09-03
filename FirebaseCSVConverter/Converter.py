"""
Created on 2018-08-31

@author: andrew wyld
"""

import json
import csv
import glob
import datetime

# "event_params": [
#       {
#         "key": "firebase_conversion",
#         "value": {
#           "string_value": null,
#           "int_value": "1",
#           "float_value": null,
#           "double_value": null
#         }
#       }
#     ]
__key_event_params = "event_params"


#     "user_properties": [
#       {
#         "key": "first_open_time",
#         "value": {
#           "string_value": null,
#           "int_value": "1534950000000",
#           "float_value": null,
#           "double_value": null,
#           "set_timestamp_micros": "1534946530664000"
#         }
#       }
#     ]
__key_user_properties = "user_properties"


#     "device": {
#       "category": "tablet",
#       "mobile_brand_name": "Apple",
#       "mobile_model_name": "iPad Air 2",
#       "mobile_marketing_name": null,
#       "mobile_os_hardware_model": "iPad5,3",
#       "operating_system": "IOS",
#       "operating_system_version": "11.2.6",
#       "vendor_id": "70FDAF07-A22D-4170-957B-C04EAD9D9F1D",
#       "advertising_id": null,
#       "language": "en-gb",
#       "is_limited_ad_tracking": "No",
#       "time_zone_offset_seconds": "3600",
#       "browser": null,
#       "browser_version": null
#     }
__key_device = "device"
__key_d_category = "category"
__key_d_brand = "mobile_brand_name"
__key_d_model = "mobile_model_name"
__key_d_marketing = "mobile_marketing_name"
__key_d_os_hw = "mobile_os_hardware_model"
__key_d_os_sw = "operating_system"
__key_d_os_ver = "operating_system_version"
__key_d_vendor_id = "vendor_id"
__key_d_ad_id = "advertising_id"
__key_d_lang = "language"
__key_d_lim_ad = "is_limited_ad_tracking"
__key_d_tz_offset_s = "time_zone_offset_seconds"
__key_d_browser = "browser"
__key_d_browser_ver = "browser"

__subkeys_device = [
    __key_d_category,
    __key_d_brand,
    __key_d_model,
    __key_d_marketing,
    __key_d_os_hw,
    __key_d_os_sw,
    __key_d_os_ver,
    __key_d_vendor_id,
    __key_d_ad_id,
    __key_d_lang,
    __key_d_lim_ad,
    __key_d_tz_offset_s,
    __key_d_browser,
    __key_d_browser_ver
]


#     "geo": {
#       "continent": "Europe",
#       "country": "United Kingdom",
#       "region": "Wales",
#       "city": "Newport",
#       "sub_continent": null,
#       "metro": null
#     }
__key_geo = "geo"
__key_g_continent = "continent"
__key_g_country = "country"
__key_g_region = "region"
__key_g_city = "city"
__key_g_sub_continent = "sub_continent"
__key_g_metro = "metro"

__subkeys_geo = [
    __key_g_continent,
    __key_g_country,
    __key_g_region,
    __key_g_city,
    __key_g_sub_continent,
    __key_g_metro
]


#     "app_info": {
#       "id": "uk.co.skoda.assistant.ios",
#       "version": "1.1.6",
#       "install_store": null,
#       "firebase_app_id": "1:571703561616:ios:777087d7f78ec12a",
#       "install_source": "manual_install"
#     }
__key_app_info = "app_info"
__key_a_id = "id"
__key_a_ver = "version"
__key_a_ist = "install_store"
__key_a_fai = "firebase_app_id"
__key_a_iso = "install_source"

__subkeys_app_info = [
    __key_a_id,
    __key_a_ver,
    __key_a_ist,
    __key_a_fai,
    __key_a_iso
]


#     "traffic_source": {
#       "name": "(direct)",
#       "medium": "(none)",
#       "source": "(direct)"
#     }
__key_traffic_source = "traffic_source"
__key_t_name = "name"
__key_t_medium = "medium"
__key_t_source = "source"

__subkeys_traffic_source = [
    __key_t_name,
    __key_t_medium,
    __key_t_source
]


# common keys for structured elements
__key_key = "key"
__key_value = "value"
__key_sv = "string_value"
__key_iv = "int_value"
__key_fv = "float_value"
__key_dv = "double_value"
__key_ts_mic = "set_timestamp_micros"


# array of top-level keys to regard as structured
__keys_structured = [
    __key_event_params,
    __key_user_properties,
    __key_device,
    __key_geo,
    __key_app_info,
    __key_traffic_source
]


def parseJson(json_root, csv_file):
    __keys = []
    __subkeys = {}
    __subkeys[__key_event_params] = []
    __subkeys[__key_user_properties] = []
    __subkeys[__key_app_info] = __subkeys_app_info
    __subkeys[__key_device] = __subkeys_device
    __subkeys[__key_geo] = __subkeys_geo
    __subkeys[__key_traffic_source] = __subkeys_traffic_source

    for json_object in json_root:
        for key in json_object:
            if key not in __keys_structured:
                if key not in __keys:
                    __keys.append(key)
                    if "timestamp" in key and "offset" not in key:
                        key2 = key.replace("timestamp", "iso8601")
                        __keys.append(key2)
            else:
                if key == __key_event_params or key == __key_user_properties:
                    sublist = json_object[key]
                    for json_subobject in sublist:
                        key2 = json_subobject[__key_key]
                        if key2 not in __subkeys[key]:
                            __subkeys[key].append(key2)

    with open(csv_file, 'w') as outputFile:
        writeCsv(json_root, __keys, __subkeys, outputFile)


def writeCsv(json_list, __keys, __subkeys, outputFile):
    writer = csv.writer(outputFile, dialect='excel')

    headings = __keys \
               + __subkeys[__key_event_params] \
               + __subkeys[__key_user_properties] \
               + __subkeys[__key_app_info] \
               + __subkeys[__key_device] \
               + __subkeys[__key_geo] \
               + __subkeys[__key_traffic_source]
    writer.writerow(headings)

    """ write data """
    for json_object in json_list:
        row = []
        for key in __keys:
            row.append(tryGet(json_object, key))


        getSubElements(__subkeys, __key_event_params, json_object, row)
        getSubElements(__subkeys, __key_user_properties, json_object, row)
        getStructElements(__subkeys, __key_app_info, json_object, row)
        getStructElements(__subkeys, __key_device, json_object, row)
        getStructElements(__subkeys, __key_geo, json_object, row)
        getStructElements(__subkeys, __key_traffic_source, json_object, row)

        writer.writerow(row)

    return


def getSubElements(__subkeys, treekey, json_object, row):
    tree = json_object[treekey]
    treedict = {}
    for key in __subkeys[treekey]:
        treedict[key] = ""
    if tree is not None:
        for element in tree:
            treedict[element[__key_key]] = element
    for key in __subkeys[treekey]:
        root = treedict[key]
        value = root[__key_value] if __key_value in root else None
        output = ""
        if value is not None:
            strang = value[__key_sv] if __key_sv in value else None
            anteger = value[__key_iv] if __key_iv in value else None
            floot = value[__key_fv] if __key_fv in value else None
            dibble = value[__key_dv] if __key_dv in value else None
            timstomp = value[__key_ts_mic] if __key_ts_mic in value else None
            if timstomp is not None:
                output = timstomp
            elif strang is not None:
                output = strang
            elif anteger is not None:
                output = anteger
            elif floot is not None:
                output = floot
            elif dibble is not None:
                output = dibble
        row.append(output)


def getStructElements(__subkeys, treekey, json_object, row):
    tree = json_object[treekey]
    for key in __subkeys[treekey]:
        row.append(tryGet(tree, key))


def tryGet(json_object, key):
    if key in json_object:
        return json_object[key]
    elif "iso8601" in key:
        timestamp = tryGet(json_object, key.replace("iso8601", "timestamp"))
        if timestamp is not None and timestamp != '':
            print(timestamp[:-6])
            return datetime.date.fromtimestamp(int(timestamp[:-6])).isoformat()
        else:
            return ''
    else:
        return ''

if __name__ == '__main__':
    pass

json_file_iterator = glob.iglob('*.json')


while True:
    try:
        json_file = next(json_file_iterator)
        csv_file = json_file[:-5] + ".csv"

        with open(json_file, 'r') as inputFile:

            json_root = json.load(inputFile)
            parseJson(json_root, csv_file)

    except StopIteration:
        break
