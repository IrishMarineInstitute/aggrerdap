# aggrerdapp
Takes an ERDDAP tabledap dataset and Aggregates the data into a new time aggregated dataset

To update the aggregations for two tabledaps for two months each, something like this might work:
```
for device in galway_obs_fluorometer spiddal_obs_ctd
do for date in 2017-11 2017-12
    do for period in daily hourly
       do echo ./aggrerddap.py $device $date $period
           ./aggrerddap.py $device $date $period
       done
    done
done

```
