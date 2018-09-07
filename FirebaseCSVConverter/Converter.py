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

# common keys for segmentation
__key_user_id = "user_id"
__key_event_name = "event_name"
__key_event_login = "AnalyticsEventLogin"
__key_user_retailer = "user_retailer"



def __segment(json_root, keys):

    tuples = []
    for key in keys:
        tuples.append([key, {}])

    for tuple in tuples:
        for json_object in json_root:
            uid = json_object[tuple[0]]
            keyed_events = tuple[1].get(uid, None)
            if keyed_events is None:
                keyed_events = []
                tuple[1][uid] = keyed_events
            keyed_events.append(json_object)

    return tuples


def parseJson(json_structured):
    __keys = []
    __subkeys = {}
    __subkeys[__key_event_params] = []
    __subkeys[__key_user_properties] = []
    __subkeys[__key_app_info] = __subkeys_app_info
    __subkeys[__key_device] = __subkeys_device
    __subkeys[__key_geo] = __subkeys_geo
    __subkeys[__key_traffic_source] = __subkeys_traffic_source

    for json_object in json_structured:
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

    return flatten(json_structured, __keys, __subkeys)


def writeCsvs(headings, json_root, csv_file_mantissa, csv_file_extender):

    with open(csv_file_mantissa + csv_file_extender, 'w') as outputFile:
        writeCsv(headings, json_root, outputFile)

    #    by_user, by_city, by_event, by_session, by_centre = __segment(json_root)
    tuples = __segment(json_root, [__key_user_id, __key_event_name, __key_user_retailer])

    for tuple in tuples:
        for key in tuple[1].keys():
            if key is not None:
                collection = tuple[1][key]
                with open(csv_file_mantissa + "_" + tuple[0] + "_" + key + "_" + csv_file_extender, 'w') as outputFile:
                    writeCsv(headings, collection, outputFile)

    keypairs = [
        ["button_press", "button_name"],
        ["user_engagement", "firebase_screen_class"],
        ["screen_view", "firebase_screen_class"]
    ]

    __output_digests(json_root, keypairs)


def __output_digests(json_root, keypairs):
    namesets = {}
    counts = {}
    count_per_user = {}
    for json_object in json_root:
        uid = json_object[__key_user_id]
        if uid not in count_per_user.keys():
            count_per_user[uid] = {}

        for keypair in keypairs:
            if json_object[__key_event_name] == keypair[0]:

                name = json_object[keypair[1]]
                name_qualified = keypair[0] + "_" + name

                if name_qualified not in namesets.keys():
                    namesets[name_qualified] = [keypair[0], name]

                if name_qualified not in count_per_user[uid].keys():
                    count_per_user[uid][name_qualified] = 1
                else:
                    count_per_user[uid][name_qualified] += 1

                if name_qualified not in counts.keys():
                    counts[name_qualified] = 1
                else:
                    counts[name_qualified] += 1
    # output files per user
    for user in count_per_user.keys():
        with open("some_events_" + user + ".csv", 'w') as outputFile:
            writer = csv.writer(outputFile, dialect='excel')
            writer.writerow(["name", "subtype" "count"])
            for key in count_per_user[user].keys():
                rowarray = [namesets[key][0], namesets[key][1], count_per_user[user][key]]
                writer.writerow(rowarray)
    # output file indexed by user
    with open("some_events_users.csv", 'w') as outputFile:
        writer = csv.writer(outputFile, dialect='excel')
        writer.writerow(["user", "name", "subtype", "count"])
        for user in count_per_user.keys():
            for key in count_per_user[user].keys():
                rowarray = [user, namesets[key][0], namesets[key][1], count_per_user[user][key]]
                writer.writerow(rowarray)
    # output total
    with open("some_events.csv", 'w') as outputFile:
        writer = csv.writer(outputFile, dialect='excel')
        writer.writerow(["name", "subtype", "count"])
        for key in counts.keys():
            rowarray = [namesets[key][0], namesets[key][1], counts[key]]
            writer.writerow(rowarray)


def __propagate(__event_primary, __keys_primary, __key_primary, json_root):

    __propaganda = []
    for thang in __keys_primary:
        __propaganda.append([thang, {}])

    # prime the pump
    for json_object in json_root:
        id = json_object[__key_primary]
        if json_object[__key_event_name] == __event_primary:
            for __propagandum in __propaganda:
                if id not in __propagandum[1].keys():
                    __propagandum[1][id] = json_object[__propagandum[0]]

    # push values down
    for json_object in json_root:
        id = json_object[__key_primary]

        if json_object[__key_event_name] == __event_primary:
            for __propagandum in __propaganda:
                __propagandum[1][id] = json_object[__propagandum[0]]
        else:
            for __propagandum in __propaganda:
                if id in __propagandum[1].keys():
                    json_object[__propagandum[0]] = __propagandum[1][id]
                else:
                    json_object[__propagandum[0]] = __event_primary + " not captured"


def writeCsv(headings, rows, outputFile):
    writer = csv.writer(outputFile, dialect='excel')
    writer.writerow(headings)
    for row in rows:
        rowarray = []
        for heading in headings:
            if heading in row.keys():
                rowarray.append(row[heading])
            else:
                rowarray.append("")
        writer.writerow(rowarray)

def flatten(json_list, __keys, __subkeys):
    headings = __keys \
               + __subkeys[__key_event_params] \
               + __subkeys[__key_user_properties] \
               + __subkeys[__key_app_info] \
               + __subkeys[__key_device] \
               + __subkeys[__key_geo] \
               + __subkeys[__key_traffic_source]

    output = []

    """ write data """
    for json_object in json_list:
        row = {}
        for key in headings:
            row[key] = tryGet(json_object, key)

        getSubElements(__subkeys, __key_event_params, json_object, row)
        getSubElements(__subkeys, __key_user_properties, json_object, row)
        getStructElements(__subkeys, __key_app_info, json_object, row)
        getStructElements(__subkeys, __key_device, json_object, row)
        getStructElements(__subkeys, __key_geo, json_object, row)
        getStructElements(__subkeys, __key_traffic_source, json_object, row)

        output.append(row)

    return headings, output

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
            if strang is not None:
                output = strang
            elif timstomp is not None:
                output = timstomp
            elif anteger is not None:
                output = anteger
            elif floot is not None:
                output = floot
            elif dibble is not None:
                output = dibble
        row[key] = output


def getStructElements(__subkeys, treekey, json_object, row):
    tree = json_object[treekey]
    for key in __subkeys[treekey]:
        row[key] = tryGet(tree, key)


def tryGet(json_object, key):
    if key in json_object:
        return json_object[key]
    elif "iso8601" in key:
        timestamp = tryGet(json_object, key.replace("iso8601", "timestamp"))
        if timestamp is not None and timestamp != '':
            # print(timestamp[:-6])
            return datetime.datetime.fromtimestamp(int(timestamp[:-6])).isoformat()
        else:
            return ''
    else:
        return ''

if __name__ == '__main__':
    pass

json_file_iterator = glob.iglob('*.json')

headings = []
json_root = []

mantissa = "firebase"
extender = ".csv"

while True:
    try:
        json_file = next(json_file_iterator)

        with open(json_file, 'r') as inputFile:

            jsondata = json.load(inputFile)
            __headings, __json_root = parseJson(jsondata)

            for heading in __headings:
                if heading not in headings:
                    headings.append(heading)

            json_root.extend(__json_root)

    except StopIteration:
        break

json_root_sorted = sorted(json_root, key=lambda element: element["event_timestamp"])
__propagate(__key_event_login, ["user_retailer", "user_area"], __key_user_id, json_root_sorted)
writeCsvs(headings, json_root_sorted, mantissa, extender)