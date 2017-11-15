#!/usr/bin/env python
from __future__ import print_function
from erddap import erddap, filtered
from datetime import datetime, date, timedelta
import sqlite3
import pandas as pd
import argparse
import sys
import os
import xarray as xr
import inflection
import math
from xml.sax.saxutils import escape
import shutil
 
class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1
 
    def step(self, value):
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1
 
    def finalize(self):
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))
  
def new_erddap():
   return erddap("http://erddap.marine.ie/erddap")

def translate_type(s):
   if s.name in ["float16", "float32"]:
     return "float"
   if s.name.startswith("float"):
     return "double"
   if s.name.startswith("int"):
     return "int"
   return "String"

def nextmonth(x):
  try:
    return x.replace(month=x.month+1)
  except ValueError:
    if x.month == 12:
      return x.replace(year=x.year+1, month=1)
    else:
      # next month is too short to have "same date"
      # pick your own heuristic, or re-raise the exception:
      raise

def mdvars(seq,match):
  for o in seq:
    wanted = True
    for k in match:
       if k in o and o[k] == match[k] :
          pass
       else:
          wanted = False
    if wanted: yield o

def aggregate(ts,min_date,period,data_dir,dataset_dir,constraints):
    max_date = nextmonth(min_date)
    sqlite = ts.sqlite()
    # or should everything be put in one database...
    dbfile = '/var/tmp/{0}_{1}.db'.format(sqlite.table_name,period)
    try:
      os.remove(dbfile)
    except OSError:
      pass
    conn = sqlite3.connect(dbfile)
    conn.create_aggregate("stdev",1,StdevFunc)

    c = conn.cursor()
    #print(sqlite.sql_create_table())
    c.execute(sqlite.sql_create_table())
    query = sqlite.sql_insert()
    #print(query)
    batch = []
    i = 0
    for o in ts.data(min_date,max_date,constraints):
       i = i + 1
       t = sqlite.tuplify(o)
       batch.append(t)
       if i % 1000 == 0:
         #print('{:%Y-%m-%d %H:%M:%S} {}'.format(datetime.now(),i), end="\r")
         c.executemany(query,batch)
         batch = []
         if i % 500000 == 0:
             conn.commit()
    if len(batch):
       c.executemany(query,batch)
    primary_keys = ','.join([o["name"] for o in filtered(sqlite.columns,{"key": True})])
    sqlidx = "create index idx_{0}_{1} on {0}({2},{1})".format(sqlite.table_name,period,primary_keys);
    c.execute(sqlidx);
    conn.commit()
    df = pd.read_sql(sqlite.sql_aggregate(period),conn);
    conn.close()
    # create xarray Dataset from Pandas DataFrame
    encoding = {}
    for column in df:
      if column.endswith("time"):
        df[column] = pd.to_datetime(df[column],infer_datetime_format=True)
        encoding[column] = {
              "units": "seconds since 1970-01-01T00:00:00Z" }
    xds = xr.Dataset.from_dataframe(df)
    summary = ts.summary()
    xds.attrs.update(summary)
    if "title" in summary:
      xds.attrs.update({"title": "{0}{1} aggregations of {2}".format(period[0].upper(),period[1:],summary["title"])})

    metadata = [x for x in ts.metadata()]
    for v in metadata:
       if v["Row Type"] == "attribute":
         value = v["Value"]
         lcv = inflection.underscore(v["Variable Name"])
         if v["Attribute Name"] == "units" and lcv.endswith("time"):
            continue
         if lcv in xds:
           xds[lcv].attrs.update({v["Attribute Name"]: v["Value"]})
         for agg in ["mean","stdev","maximum","minimum"]:
           k = "{0}_{1}".format(agg,lcv)
           v2 = v["Value"]
           if v["Attribute Name"] in ["long_name","standard_name"]:
              v2 = "{0} {1}".format(agg,v2)
           if k in xds:
              xds[k].attrs.update({v["Attribute Name"]: v2})

    dataset_id = "{0}_{1}".format(sqlite.table_name,period)
    filedir = "{0}/{1}/".format(data_dir,dataset_id)
    filepath = "{0}{1}.nc".format(filedir,min_date.strftime("%Y/{0}_%Y_%m_%d".format(dataset_id)))
    directory = os.path.dirname(filepath)
    if not os.path.exists(directory):
      os.makedirs(directory)
    configpath = "{0}/{1}.part".format(dataset_dir,dataset_id)
    directory = os.path.dirname(configpath)
    if not os.path.exists(directory):
      os.makedirs(directory)

    erdds = [
    """<dataset type="EDDTableFromNcFiles" datasetID="{0}" active="true">
    <reloadEveryNMinutes>1440</reloadEveryNMinutes>
    <updateEveryNMillis>10000</updateEveryNMillis>
    <fileDir>{1}</fileDir>
    <fileNameRegex>.*.nc</fileNameRegex>
    <recursive>true</recursive>
    <pathRegex>.*</pathRegex>
    <metadataFrom>last</metadataFrom>
    <fileTableInMemory>false</fileTableInMemory>
    <accessibleViaFiles>true</accessibleViaFiles>
    <removeMVRows>true</removeMVRows>
    <!--
    <onChange></onChange>
    -->
    <addAttributes>
    """.format(dataset_id,filedir)
    ]
    for k,v in xds.attrs.iteritems():
      erdds.append('      <att name="{0}">{1}</att>'.format(escape(k),escape(v)))

    erdds.append("    </addAttributes>")

    i = 0
    for name,variable in xds.variables.iteritems():
      if i == 0 and name == "index":
         continue
      i = i + 1
      if name != "time" and name.endswith("time"):
        variable.attrs.update({
             "ioos_category": "Time",
             "long_name": name,
             "standard_name": name,
             "time_origin": "01-JAN-1970 00:00:00"
            })

      erdds.append("    <dataVariable>")
      erdds.append("     <sourceName>{0}</sourceName>".format(name))
      erdds.append("     <destinationName>{0}</destinationName>".format(name))
      erdds.append("     <dataType>{0}</dataType>".format(translate_type(variable.dtype)))
      erdds.append("     <addAttributes>")
      for k,v in variable.attrs.iteritems():
            erdds.append('      <att name="{0}">{1}</att>'.format(escape(k),escape(v)))
      erdds.append("     </addAttributes>")
      erdds.append("    </dataVariable>")

    erdds.append("</dataset>")

    xds.to_netcdf("{0}.tmp".format(filepath),encoding=encoding)
    shutil.move("{0}.tmp".format(filepath),filepath)
    with open(configpath,"w") as out:
       out.write("\n".join(erdds))


def valid_date(s):
    if len(s) == len("2016-01"):
      try:
        return datetime.strptime("{0}-01T00:00:00Z".format(s) , "%Y-%m-%dT%H:%M:%SZ" )
      except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

    msg = "Not a valid date in format YYYY-MM: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("series",help="The timeseries identifier in erddap")
   parser.add_argument("startdate", help="Start date format YYYY-MM",  type=valid_date)
   parser.add_argument("period", choices=['hourly','daily','monthly'])
   parser.add_argument("--data_dir", help="Folder containing the netcdf files", default="/opt/aggrerddap/data")
   parser.add_argument("--dataset_dir", help="Folder containing the erdap dataset files", default="/opt/aggrerddap/config")
   parser.add_argument('constraints', nargs = '*', help = 'any constraints included in the query eg, "temp<=25"')
   args = parser.parse_args()
   erddap = new_erddap()
   timeseries = {ts.id: ts for ts in erddap.timeseries()}
   if(args.series not in timeseries):
     print("unknown timeseries {0}, try one of these: [{1}]".format(args.series, ", ".join(timeseries.keys())))
     sys.exit(2)
   min_date = date(args.startdate.year,args.startdate.month,args.startdate.day)
   aggregate(timeseries[args.series],min_date,args.period,args.data_dir,args.dataset_dir,args.constraints)
