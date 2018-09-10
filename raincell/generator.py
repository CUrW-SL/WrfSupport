import datetime as dt
import glob
import os
import shutil
import tempfile
import json
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.resources import manager as res_mgr
from curw.rainfall.wrf.extraction import observation_utils as wrf_utils
from curwmysqladapter import MySQLAdapter


def get_curw_adapter(mysql_config=None, mysql_config_path=None):
    if mysql_config_path is None:
        mysql_config_path = res_mgr.get_resource_path('config/mysql_config.json')

    with open(mysql_config_path) as data_file:
        config = json.load(data_file)

    if mysql_config is not None and isinstance(mysql_config, dict):
        config.update(mysql_config)

    return MySQLAdapter(**config)

def download_raincell_file(adapter, net_cdf_file_name, start_ts_lk, duration_days):
    #start_ts_lk = '2018-05-24_08:00'
    obs_stations = {'Kottawa North Dharmapala School': [79.95818, 6.865576, 'A&T Labs', 'wrf_79.957123_6.859688'],
                    'IBATTARA2': [79.919, 6.908, 'CUrW IoT', 'wrf_79.902664_6.913757'],
                    'Malabe': [79.95738, 6.90396, 'A&T Labs', 'wrf_79.957123_6.913757'],
                    # 'Mutwal': [79.8609, 6.95871, 'A&T Labs', 'wrf_79.875435_6.967812'],
                    'Mulleriyawa': [79.941176, 6.923571, 'A&T Labs', 'wrf_79.929893_6.913757'],
                    'Orugodawatta': [79.87887, 6.943741, 'CUrW IoT', 'wrf_79.875435_6.940788']}

    kelani_lower_basin_points = None
    try:
        wrf_utils.extract_kelani_basin_rainfall_flo2d_with_obs(net_cdf_file_name, adapter, obs_stations,
                                                       '/home/hasitha/PycharmProjects/WrfSupport/output', start_ts_lk,
                                                 kelani_lower_basin_points=kelani_lower_basin_points,
                                                 duration_days=duration_days)
    except Exception as ex:
        print("download_raincell_file|Exception: ", ex)


try:
    mysql_conf_path = '/home/curw/Desktop/2018-05/mysql.json'
    adapter = get_curw_adapter(mysql_config_path='/home/hasitha/PycharmProjects/WrfSupport/resources/mysql.json')
    net_cdf_file = '/home/hasitha/PycharmProjects/WrfSupport/input/wrfout_d03_2018-09-09_18_00_00_rf'
    duration_days = (int(2), int(3))
    download_raincell_file(adapter, net_cdf_file, '2018-09-10_00:00',duration_days)
except Exception as e:
    print(e)