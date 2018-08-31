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

This tool gets *every* key received, stores them in a list, uses these as headings and then prints whatever value is available for each key on each row, leaving a blank space if none is present (so the columns all match up, see).

## Device, app info, geo, traffic
These are some small structures in the overall Firebase analytics object. These are unpacked to their individual elements, which, again, are all printed individually in the CSV.

## Other fixed keys
These are automatically traversed and displayed, like those in the event params and user properties, but without having to retrieve the value from a substructure.

## CSV flavour
This outputs Excel-flavoured CSV.

## Operation
Install Python 3. Try running Converter.py in a directory with a bunch of Firebase analytics JSON in; the tool will try and output a CSV file per JSON file to the same location.

This thing requires json, csv and glob to run, so pip install any of those that Python declares to be missing, baffling, or otherwise confusing.

While this *should* work on Pythons 2 and 3, I have only actually run it on Python 3.6, so no guarantees.

Behaviour on non-Firebase analytics JSON is unknown and if you have CSV in the directory already, this thing will overwrite it brutally, probably. Be careful.
