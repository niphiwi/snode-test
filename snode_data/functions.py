import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import datetime



def load(nodes=["HRM-01", "HRM-02", "HRM-03", "HRM-04", "HRM-05", "HRM-06", "HRM-07", "HRM-08", 
                "HRM-09", "HRM-10", "HRM-11", "HRM-12", "HRM-13", "HRM-14", "HRM-15", "HRM-16"], 
        start="2021-05-01", 
        end="2021-05-02",
        root="/home/nwinkler/git/sensing-node-data/logs/"):
    """Load sensing node data into pandas DataFrame.
    
    Keyword arguments:
    nodes -- the list of the desired sensing nodes (default: all HRM nodes)
    start -- the desired start date in YYYY-MM-DD format (default: "2021-05-01")
    end -- the desired end date in YYYY-MM-DD format (default: "2021-05-02")
    root -- the directory with the log files of the sensing nodes
    """

    start = datetime.datetime.strptime(start, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    delta = end - start

    df = pd.DataFrame()

    # Add each node of the nodes list to the df
    for node in nodes:
        ### REMINDER: Check path ###
        node_path = os.path.join(root, node + "/")

        file_list = []
        for i in range(delta.days + 1):
            day = start + datetime.timedelta(days=i)
            file = node_path + day.strftime("%Y_%m_%d") + '.tsv'
            if (os.path.isfile(file)):
                file_list.append(file)

        if file_list:
            df_from_each_file = (pd.read_csv(f,
                                             sep='\t',
                                             header=None,
                                             names=[
                                                 "time", "id", "fan", "temp_dht", "temp_dgs", "hum_dht", "hum_dgs", "dust", "dgs"]
                                             ) for f in file_list)
            df_per_node = pd.concat(df_from_each_file, ignore_index=True)

            df = df.append(df_per_node, ignore_index=True)

    if not df.empty:
        # Create a datetime column
        df["datetime"] = df["time"].apply(datetime.datetime.utcfromtimestamp)

        df.dust = df.dust.astype('float')

        # Make ID column categorical
        df["id"] = pd.Categorical(df["id"])

    return df


def remove_outliers(df):
    # Ommit all rows where humidity is at a unreasonable value
    hum = np.array(df['hum_dht'].values.tolist())
    df['hum_dht'] = np.where(hum > 100, np.NaN, hum).tolist()

    dgs = np.array(df['dgs'].values.tolist())
    df['dgs'] = np.where(dgs == 0, np.NaN, dgs).tolist()
    dgs = np.array(df['dgs'].values.tolist())
    df['dgs'] = np.where(dgs > 1e6, np.NaN, dgs).tolist()

    return df


def resample(df, interval="1min"):
    """Resample the DataFrame by the specified interval and

    Keyword arguments:
    df -- the DataFrame to resample
    interval -- the interval by which the DataFrame should be resampled (default: "1min")
    """
    df.index = pd.DatetimeIndex(df.datetime)
    df = df.drop(columns=['fan', 'time'])
    # Resample df every minute
    df = df.groupby('id').resample(interval).mean().interpolate('linear')

    return df.reset_index()


def add_rolling_mean(df, window):
    df["dust_rolling"] = df.dust.rolling(
        window, center=False, min_periods=1).mean()
    df["dgs_rolling"] = df.dgs.rolling(
        window, center=False, min_periods=1).mean()
    df["hum_dht_rolling"] = df.hum_dht.rolling(
        window, center=False, min_periods=1).mean()

    return df


def calculate_ppm(df, gas):
    if (gas=='iaq'):
        a = 0.0055
        b = -72.3        
    elif (gas=='co'):
        a = 0.0222
        b = -293.8
        
    df['dgs'] = a * df.dgs + b
    
    return df
