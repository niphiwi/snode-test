import pandas as pd
import os
import numpy as np
import datetime

class snode_df():
    """Load  
    
    """
    def __init__(self, root, nodes, start, end):
        self.root = root
        self.nodes = nodes
        self.start = start
        self.end = end

        delta = end - start
        df = pd.DataFrame()

        # Add each node of the nodes list to the df
        for node in nodes:
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
                                                    "unixtime", "id", "fan", "temp_dht", "temp_dgs", "hum_dht", "hum_dgs", "dust", "dgs"]
                                                ) for f in file_list)
                df_per_node = pd.concat(df_from_each_file, ignore_index=True)

                df = df.append(df_per_node, ignore_index=True)

        if not df.empty:
            # Change datatype of columns
            df["datetime"] = df["unixtime"].apply(datetime.datetime.utcfromtimestamp)
            df.dust = df.dust.astype('float')
            df["id"] = pd.Categorical(df["id"])

        self.df = df

        return None

    def get_df(self):
        return self.df

    def remove_outliers(self):
        # Ommit all rows where humidity is at a unreasonable value
        hum = np.array(self.df['hum_dht'].values.tolist())
        self.df['hum_dht'] = np.where(hum > 100, np.NaN, hum).tolist()

        dgs = np.array(self.df['dgs'].values.tolist())
        self.df['dgs'] = np.where(dgs == 0, np.NaN, dgs).tolist()
        dgs = np.array(self.df['dgs'].values.tolist())
        self.df['dgs'] = np.where(dgs > 1e6, np.NaN, dgs).tolist()

    def resample(self, interval="1min"):
        self.df = self.df.set_index(pd.DatetimeIndex(self.df.datetime))
        self.df = self.df.drop(columns=['fan', 'time'])
        # Resample df every minute
        self.df = self.df.groupby('id').resample(interval).mean().interpolate('linear')
        self.df = self.df.reset_index()

    def add_rolling_mean(self, window):
        self.df["dust_rolling"] = self.df.dust.rolling(window, center=False, min_periods=1).mean()
        self.df["dgs_rolling"] = self.df.dgs.rolling(window, center=False, min_periods=1).mean()
        self.df["hum_dht_rolling"] = self.df.hum_dht.rolling(window, center=False, min_periods=1).mean()


    def calculate_ppm(self, gas):
        if (gas=='iaq'):
            a = 0.0055
            b = -72.3        
        elif (gas=='co'):
            a = 0.0222
            b = -293.8
            
        self.df['dgs'] = a * self.df.dgs + b
