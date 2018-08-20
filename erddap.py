from __future__ import print_function
import requests
import inflection
import time
import os
from datetime import date, datetime, timedelta
from contextlib import closing
import csv
import urllib

def cassandra_type(t):
   known = {"String": "text"}
   if t in known:
      return known[t]
   return t

def get_dates(start,end,days=1):
  dates = []
  while start < end:
       dates.append("{0}T00:00:00Z".format(start.isoformat()))
       start = start + timedelta(days=days)
  dates.append("{0}T00:00:00Z".format(end.isoformat()))
  return dates
    
def remap_tabledap(tabledap):
    table = tabledap["table"]
    columnNames = table["columnNames"]
    answer = []
    for row in table["rows"]:
       o = {}
       for idx, val in enumerate(columnNames):
         o[val] = row[idx]
       answer.append(o)
    return answer

def tabledap(url):
   r = requests.get(url) 
   if r.status_code == 200:
     return remap_tabledap(r.json())
   else:
     return []

def filtered(seq,match):
  for o in seq:
    wanted = True
    for k in match:
       if k in o and o[k] == match[k] :
          pass
       else:
          wanted = False
    if wanted: yield o

def parse_iso_timestamp(timestamp):
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ" )

class timeseries():
  _metadata = None
  _variables = None
  _min_time = None
  id = None
  def __init__(self,info,namespace="ts"):
    self.info = info
    self.id = info["datasetID"]
    self.namespace = namespace

  def metadata(self):
     if not self._metadata:
        url = "{0}.json".format(self.info["metadata"])
        self._metadata = tabledap(url)
     return self._metadata

  def tabledap_url(self):
        return "{0}.json".format(self.info["tabledap"])

  def data(self,min_date=None,max_date=None,constraints=[]):
    timecol = self.time_column()
    if min_date is None:
      mt = parse_iso_timestamp(self.min_time())
      min_date = date(mt.year,mt.month,mt.day)
      if min_date > date(2000,01,01):
         min_date = date(2000,01,01)
    if max_date is None:
      max_date = date.today() + timedelta(days=365)
    
    dates = get_dates(min_date,max_date,days=60)
    variables = self.variables()
    sconstraints = ""
    print(constraints)
    if constraints and len(constraints):
      sconstraints = "&{0}".format("&".join([urllib.quote_plus(c) for c in constraints]))
    base_url = "{0}.csv?{1}&{2}>={3}&{2}<{4}{5}".format(self.info["tabledap"],",".join([v["name"] for v in variables]),timecol,"{0}","{1}",sconstraints)
    for d in range(len(dates)-1):
      start = dates[d]
      end = dates[d+1]
      url = base_url.format(start,end)
      print(url)
      with closing(requests.get(url, stream=True)) as r:
        if r.status_code == 200:
          # reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"')
          reader = csv.reader(r.iter_lines())
          i = 0
          for row in reader:
            i = i + 1
            if i<=2:
              continue
            o = {}
            for n in range(len(row)):
               v = row[n]
               variable = variables[n]
               if variable["cassandra_type"] in ["float","double","int"]:
                  if v == "NaN":
                    o[variable["lcname"]] = None
                  elif variable["cassandra_type"] in ["float","double"]:
                    o[variable["lcname"]] = float(v)
                  else:
                    o[variable["lcname"]] = int(v)
                    
               else:
                 o[variable["lcname"]] = v
              
            yield o

  def time_column(self):
    for v in self.variables():
      if v["lcname"] == "time":
        return v["name"]
    return None

  def min_time(self):
    summary = self.summary()
    if "time_coverage_start" in summary:
       return summary["time_coverage_start"]

    if self._min_time is None:
      timecol = self.time_column()
      dates = get_dates(date(1990,1,1),date.today() + timedelta(days=1))
      self._min_time = dates[0]
      first = 0
      last = len(dates)-2
      base_url = "{0}?{1}&{1}>={2}&{1}<={3}&orderByMin(%22{1}%22)".format(
          self.tabledap_url(),timecol,"{0}","{1}"
        )
    
      while first<=last:
        midpoint = (first + last)//2
        url = base_url.format(dates[midpoint],dates[midpoint+1])
        data = tabledap(url)
        if len(data):
          self._min_time = data[0][timecol]
          last = midpoint-1
        else:
          first = midpoint+1
      
    return self._min_time

  def summary(self):
    metadata = self.metadata()
    info = {}
    items = filtered(metadata,{"Variable Name": "NC_GLOBAL"})
    for item in items:
      info[item["Attribute Name"]] = item["Value"]
    return info

  def variables(self):
     if not self._variables:
        tr_axis = {
            "Time": {"name": "time", "type": "timestamp"},
            "T": {"name": "time", "type": "timestamp"},
            "Lon": {"name": "longitude", "type": "double"},
            "Lat": {"name": "latitude", "type": "double"},
            "Alt": {"name": "altitude", "type": "double"},
            "X": {"name": "longitude", "type": "double"},
            "Y": {"name": "latitude", "type": "double"},
        }
            #"Z": {"name": "altitude", "type": "double"}
        identifiers = [x["Variable Name"] for x in
                       filtered(self.metadata(), {
                           "Row Type": "attribute",
                           "Attribute Name": "ioos_category",
                           "Value": "Identifier"
                        })]
        quality = [x["Variable Name"] for x in
                       filtered(self.metadata(), {
                           "Row Type": "attribute",
                           "Attribute Name": "ioos_category",
                           "Value": "Quality"
                        })]
        v = filtered(self.metadata(),{"Row Type": "variable"})
        answer = [ { 
                    "name": x["Variable Name"], 
                    "lcname": inflection.underscore(x["Variable Name"]), 
                    "type": x["Data Type"],
                    "cassandra_type": cassandra_type(x["Data Type"]),
                    "identifier": False,
                    "quality": False,
                    "axis": False,
                    "units": None
                   } for x in v]
        for x in answer:
           if x["name"] in identifiers:
              x["identifier"] = True
           if x["name"] in quality:
              x["quality"] = True

           axis = [a for a in filtered(self.metadata(), {
                           "Row Type": "attribute",
                           "Attribute Name": "axis",
                           "Variable Name": x["name"]
                        })]
           if len(axis):
              x["axis"] = True
              if axis[0]["Value"] in tr_axis:
                 x["lcname"] = tr_axis[axis[0]["Value"]]["name"]
                 x["cassandra_type"] = tr_axis[axis[0]["Value"]]["type"]
           units = [a["Value"] for a in filtered(self.metadata(), {
                           "Row Type": "attribute",
                           "Attribute Name": "units",
                           "Variable Name": x["name"]
                        })]
           if len(units):
             x["units"] = units[0]

        self._variables = sorted(answer, key=lambda o: (-o["identifier"],o["lcname"]))
     return self._variables

  def base_table_name(self):
      return os.path.basename(inflection.underscore(self.info["datasetID"]))

  def suggest_table_name(self):
      return "{2}.{0}_{1}".format(self.base_table_name(),int(time.time()),self.namespace)

  def cassandra(self):
     return cassandra_table(self.suggest_table_name(),self.summary(),variables=self.variables())

  def sqlite(self):
     return sqlite3_table(self.base_table_name(),self.summary(),variables=self.variables())

class sqlite3_table():
  def __init__(self,table_name,summary,variables=None, columns=None):
    self.table_name = table_name
    self.columns = columns
    self.summary = summary
    if variables:
      self.columns = self._erddap2columns(variables)

  def tuplify(self,o):
     mt = parse_iso_timestamp(o["time"])
     o["minutely"] = mt.strftime("%Y-%m-%dT%H%M" )
     o["hourly"] = mt.strftime("%Y-%m-%dT%H" )
     o["daily"] = mt.strftime("%Y-%m-%d")
     o["weekly"] =  mt.strftime("%Y-%W")
     o["monthly"] =  mt.strftime("%Y-%m")
     answer = []
     for v in self.columns:
       answer.append(o[v["name"]])
     return tuple(answer)

  def _erddap2columns(self,variables):
     pks = []
     varnames = []
     varmap = {}
     cols = []
     for v in variables:
       varmap[v["lcname"]] = v
       if v["identifier"] or v["cassandra_type"] == "timestamp":
          cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"], "quality": v["quality"], "axis": v["axis"]})
          pks.append(v["lcname"])
       else: 
          varnames.append(v["lcname"])

     for v in ["minutely","hourly","daily","weekly","monthly"]:
       cols.append({"name": v, "type": "text", "key": False, "erddap_name": None})

     for s in ["latitude","longitude","time"]:
       if s in varnames:
         v = varmap[s]
         cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"], "quality": v["quality"], "axis": v["axis"]})
         varnames.remove(s)
     for s in varnames:
       v = varmap[s]
       cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"], "quality": v["quality"], "axis": v["axis"]})
     return cols

  def sql_insert(self):
     cols = ','.join([o["name"] for o in self.columns])
     placeholders = ','.join(["?" for o in self.columns])
     return "insert into {0} ({1}) values ({2})".format(self.table_name,cols,placeholders)

  def sql_create_table(self):
     columns = ', '.join(["{0} {1}".format(o["name"],o["type"]) for o in self.columns])
     #primary_keys = ', '.join([o["name"] for o in filtered(self.columns,{"key": True})])
     #cql = "create table {0} ({1}, PRIMARY KEY ({2}));".format(self.table_name,columns,primary_keys) 
     sql = "create table {0} ({1});".format(self.table_name,columns) 
     return sql

  def get_v_tables_part(self,keys,col,period,axis_part):
     return """
    (select {0}, {1}, min({2}) minimum_{2}, time minimum_{2}_time from {3} group by {0},{1}) {2}_minimum, 
    (select {0}, {1}, max({2}) maximum_{2}, time maximum_{2}_time from {3} group by {0},{1}) {2}_maximum, 
    (select {0}, {1}, strftime('%Y-%m-%dT%H:%M:%SZ',datetime(avg(strftime('%s',time)),'unixepoch')) mean_time, {4} stdev({2}) stdev_{2}, avg({2}) mean_{2} from {3} group by {0},{1}) {2}_mean""".format(', '.join(keys),period,col,self.table_name,axis_part)

  def get_join_part(self,keys,period,first_col,other_col):
      conditions = []
      cols = [k for k in keys]
      cols.append(period)
      for suffix in ["mean","minimum","maximum"]:
          for key in cols:
             conditions.append("{0}_mean.{3}={1}_{2}.{3}".format(first_col,other_col,suffix,key))
      return ' and '.join(conditions)
     

  def get_select_part(self,col,period):
      return "mean_{1}, stdev_{1}, minimum_{1}, minimum_{1}_time, maximum_{1}, maximum_{1}_time".format(period,col)

  def sql_aggregate(self,period):
     keys = [o["name"] for o in filtered(self.columns,{"key": True})]
     skip = [k for k in keys]
     skip.extend([o["name"] for o in filtered(self.columns,{"quality": True})])
     skip.extend(["minutely","hourly","daily","weekly","monthly","time"])
     axis = [o["name"] for o in filtered(self.columns,{"axis": True}) if o["name"] not in skip ]
     skip.extend(axis)

     columns = [o["name"] for o in self.columns if o["name"].lower() not in skip and o["type"] in ["float","double","int"]]
     first_col = columns[0]
     cols = ["{0}_mean.{1}".format(first_col,k) for k in keys+axis]
     cols.append("{0}_mean.mean_time time".format(first_col,period))
     tables = []
     conditions = []
     axis_part = ""
     if(len(axis)):
       axis_part = "{0},".format(", ".join(["avg({0}) {0}".format(a) for a in axis]))

     for col in columns:
        cols.append(self.get_select_part(col,period))
        tables.append(self.get_v_tables_part(keys,col,period,axis_part))
        axis_part = ""
        conditions.append(self.get_join_part(keys,period,first_col,col))
        
     return "select {0} from {1} where {2};".format(",\n    ".join(cols),",\n    ".join(tables),"\n     AND ".join(conditions))

class cassandra_table():
  def __init__(self,table_name,summary,variables=None, columns=None):
    self.table_name = table_name
    self.columns = columns
    self.summary = summary
    if variables:
      self.columns = self._erddap2columns(variables)

  def tuplify(self,o):
     mt = parse_iso_timestamp(o["time"])
     o["year"] = mt.year
     o["month"] = mt.month
     o["day"] = mt.day
     o["hour"] = mt.hour
     o["minute"] = mt.minute
     o["second"] = mt.second
     o["millis"] = int(round(mt.microsecond * 1000))
     o["time"] = mt
     answer = []
     for v in self.columns:
       answer.append(o[v["name"]])
     return tuple(answer)

  def _erddap2columns(self,variables):
     pks = []
     varnames = []
     varmap = {}
     cols = []
     for v in variables:
       varmap[v["lcname"]] = v
       if v["identifier"]:
          cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"], "quality": v["quality"], "axis": v["axis"]})
          pks.append(v["lcname"])
       else: 
          varnames.append(v["lcname"])
     # standard columns first
     for v in ["year","month","day","hour","minute","second","millis"]:
       cols.append({"name": v, "type": "int", "key": True, "erddap_name": None})
       if v in varnames:
         varnames.remove(v)
     for s in ["latitude","longitude","time"]:
       if s in varnames:
         v = varmap[s]
         cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"]})
         varnames.remove(s)
     for s in varnames:
       v = varmap[s]
       cols.append({"name": v["lcname"], "type": v["cassandra_type"], "key": v["identifier"], "erddap_name": v["name"]})
     return cols

  def cql_insert(self):
     cols = ','.join([o["name"] for o in self.columns])
     placeholders = ','.join(["?" for o in self.columns])
     return "insert into {0} ({1}) values ({2})".format(self.table_name,cols,placeholders)

  def cql_create_table(self):
     columns = ', '.join(["{0} {1}".format(o["name"],o["type"]) for o in self.columns])
     primary_keys = ', '.join([o["name"] for o in filtered(self.columns,{"key": True})])
     cql = "create table {0} ({1}, PRIMARY KEY ({2}));".format(self.table_name,columns,primary_keys) 
     return cql

class erddap():
  _timeseries = None
  def __init__(self,base_url):
      self.base_url = base_url

  def timeseries(self):
      if not self._timeseries:
         answer = []
         for datatype in ["TimeSeries","Point"]:
           url = "{0}/tabledap/allDatasets.json?&cdm_data_type=%22{1}%22".format(self.base_url,datatype)
           for t in tabledap(url):
              answer.append(timeseries(t))
         self._timeseries = answer

      return self._timeseries

