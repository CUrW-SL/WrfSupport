import json
import numpy as np
import pandas as pd
from curw.rainfall.wrf.extraction import utils as ext_utils
from curw.rainfall.wrf.extraction import spatial_utils
from curw.rainfall.wrf.resources import manager as res_mgr
from curw.rainfall.wrf import utils
import datetime as dt
from curwmysqladapter import MySQLAdapter


class CurwObservationException(Exception):
    pass


def get_curw_adapter(mysql_config=None, mysql_config_path=None):
    if mysql_config_path is None:
        mysql_config_path = res_mgr.get_resource_path('config/mysql_config.json')

    with open(mysql_config_path) as data_file:
        config = json.load(data_file)

    if mysql_config is not None and isinstance(mysql_config, dict):
        config.update(mysql_config)

    return MySQLAdapter(**config)


def get_observed_precip(obs_stations, start_dt, end_dt, duration_days, adapter, forecast_source='wrf0', ):
    def _validate_ts(_s, _ts_sum, _opts):
        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return

        f_station = {'station': obs_stations[_s][3],
                     'variable': 'Precipitation',
                     'unit': 'mm',
                     'type': 'Forecast-0-d',
                     'source': forecast_source,
                     }
        f_ts = np.array(adapter.retrieve_timeseries(f_station, _opts)[0]['timeseries'])

        if len(f_ts) != duration_days[0] * 24 + 1:
            raise CurwObservationException('%s Forecast time-series validation failed' % _s)

        for j in range(duration_days[0] * 24 + 1):
            d = start_dt + dt.timedelta(hours=j)
            d_str = d.strftime('%Y-%m-%d %H:00')
            if j < len(_ts_sum.index.values):
                if _ts_sum.index[j] != d_str:
                    _ts_sum.loc[d_str] = f_ts[j, 1]
                    _ts_sum.sort_index(inplace=True)
            else:
                _ts_sum.loc[d_str] = f_ts[j, 1]

        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return
        else:
            raise CurwObservationException('time series validation failed')

    obs = {}
    opts = {
        'from': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'to': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
    }

    for s in obs_stations.keys():
        print('obs_stations[s][2]: ',obs_stations[s][2])
        station = {'station': s,
                   'variable': 'Precipitation',
                   'unit': 'mm',
                   'type': 'Observed',
                   'source': 'WeatherStation',
                   'name': obs_stations[s][2]
                   }
        print('station : ', s)
        row_ts = adapter.retrieve_timeseries(station, opts)
        if len(row_ts) == 0:
            print('No data for {} station from {} to {} .'.format(s, start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        else:
            ts = np.array(row_ts[0]['timeseries'])
            print('ts length:', len(ts))
            if len(ts) != 0 :
                ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
                ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
                _validate_ts(s, ts_sum, opts)

        obs[s] = ts_sum
    return obs


def read_net_cdf(start_ts_lk, netcdf_file, duration_days, kelani_lower_basin_points=None, kelani_lower_basin_shp=None):
    if duration_days is None:
        duration_days = (2, 3)

    if kelani_lower_basin_points is None:
        #kelani_lower_basin_points = '/home/hasitha/PycharmProjects/WrfSupport/resources/local/kelani_basin_points_250m.txt'
        kelani_lower_basin_points = '/home/hasitha/PycharmProjects/WrfSupport/resources/local/klb_glecourse_points_150m.txt'

    if kelani_lower_basin_shp is None:
        kelani_lower_basin_shp = '/home/hasitha/PycharmProjects/WrfSupport/resources/shp/klb-wgs84/klb-wgs84.shp'

    points = np.genfromtxt(kelani_lower_basin_points, delimiter=',')

    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]

    diff, kel_lats, kel_lons, times = ext_utils.extract_area_rf_series(netcdf_file, kel_lat_min, kel_lat_max, kel_lon_min,
                                                                       kel_lon_max)

    def get_bins(arr):
        sz = len(arr)
        return (arr[1:sz - 1] + arr[0:sz - 2]) / 2

    lat_bins = get_bins(kel_lats)
    lon_bins = get_bins(kel_lons)

    t0 = dt.datetime.strptime(times[0], '%Y-%m-%d_%H:%M:%S')
    t1 = dt.datetime.strptime(times[1], '%Y-%m-%d_%H:%M:%S')

    print(t0)
    print(t1)
    print(lat_bins)

    obs_start = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') - dt.timedelta(days=duration_days[0])
    obs_end = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M')
    forecast_end = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') + dt.timedelta(days=duration_days[1])
    print([obs_start, obs_end, forecast_end])
    obs_stations = {'Kottawa North Dharmapala School': [79.95818, 6.865576, 'Leecom', 'wrf_79.957123_6.859688'],
                    'IBATTARA2': [79.919, 6.908, 'CUrW IoT', 'wrf_79.902664_6.913757'],
                    'Malabe': [79.95738, 6.90396, 'A&T Labs', 'wrf_79.957123_6.913757'],
                    'Mutwal': [79.8609, 6.95871, 'A&T Labs', 'wrf_79.875435_6.967812'],
                    'Mulleriyawa': [79.941176, 6.923571, 'A&T Labs', 'wrf_79.929893_6.913757'],
                    'Orugodawatta': [79.87887, 6.943741, 'CUrW IoT', 'wrf_79.875435_6.940788']}
    obs = get_observed_precip(obs_stations, obs_start, obs_end, duration_days, adapter)
    thess_poly = spatial_utils.get_voronoi_polygons(obs_stations, kelani_lower_basin_shp, add_total_area=False)
    print(thess_poly)
    output_file_path = '/home/hasitha/PycharmProjects/output/RAINCELL.DAT'
    # update points array with the thessian polygon idx
    point_thess_idx = []
    for point in points:
        point_thess_idx.append(spatial_utils.is_inside_geo_df(thess_poly, lon=point[1], lat=point[2]))
        pass

    with open(output_file_path, 'w') as output_file:
        res_mins = int((t1 - t0).total_seconds() / 60)
        data_hours = int(sum(duration_days) * 24 * 60 / res_mins)
        start_ts_lk = obs_start.strftime('%Y-%m-%d %H:%M:%S')
        end_ts = forecast_end.strftime('%Y-%m-%d %H:%M:%S')

        output_file.write("%d %d %s %s\n" % (res_mins, data_hours, start_ts_lk, end_ts))

        for t in range(int(24 * 60 * duration_days[0] / res_mins) + 1):
            for i, point in enumerate(points):
                rf = float(obs[point_thess_idx[i]].values[t]) if point_thess_idx[i] is not None else 0
                output_file.write('%d %.1f\n' % (point[0], rf))

        forecast_start_idx = int(
            np.where(times == utils.datetime_lk_to_utc(obs_end, shift_mins=30).strftime('%Y-%m-%d_%H:%M:%S'))[0])
        for t in range(int(24 * 60 * duration_days[1] / res_mins) - 1):
            for point in points:
                rf_x = np.digitize(point[1], lon_bins)
                rf_y = np.digitize(point[2], lat_bins)
                if t + forecast_start_idx + 1 < len(times):
                    output_file.write('%d %.1f\n' % (point[0], diff[t + forecast_start_idx + 1, rf_y, rf_x]))
                else:
                    output_file.write('%d %.1f\n' % (point[0], 0))


try:
    mysql_conf_path = '/home/curw/Desktop/2018-05/mysql.json'
    net_cdf_file = '/home/hasitha/PycharmProjects/WrfSupport/input/wrfout_d03_2018-09-09_18_00_00_rf'
    duration_days = (int(2), int(3))
    start_ts_lk = '2018-09-10_15:00'
    mysql_conf_path = '/home/curw/Desktop/2018-05/mysql.json'
    adapter = get_curw_adapter(mysql_config_path='/home/hasitha/PycharmProjects/WrfSupport/resources/mysql.json')
    read_net_cdf(start_ts_lk,net_cdf_file, duration_days)
except Exception as e:
    print(e)