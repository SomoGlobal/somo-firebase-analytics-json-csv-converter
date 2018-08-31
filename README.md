# somo-firebase-analytics-json-csv-converter
A little Python tool that takes Firebase analytics JSON from BigQuery and turns it into CSV.

This isn't quite as simple as it sounds, so here are a few of the constraints:

## Event params and user properties
These are lists of objects structured roughly thus:

    {
      "key": "firebase_conversion",
      "value": {
        "string_value": null,
        "int_value": "1",
        "float_value": null,
        "double_value": null
      }
    }

This tool gets *every* key received, stores them in a list, uses these as headings and then prints whatever value is available.

## Device, app info, geo, traffic
These are some small structures in the overall Firebase analytics object. These are unpacked to their individual elements, which, again, are all printed individually in the CSV.

## Other fixed keys
These are automatically traversed and displayed, like those in the event params and user properties, but without having to retrieve the value from a substructure.

## CSV flavour
This outputs Excel-flavoured CSV.
