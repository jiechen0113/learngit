#!/usr/bin/python3
# -*-coding: utf-8-*-
import os
import sys
import shutil
import json
from fastnumbers import fast_float
from datetime import date,datetime
from functools import reduce
import mysql.connector
import numpy as np
class Parser(object):
    def __init__(self,filepath,quetionable_path,save_path):
        self.filepath=filepath
        self.quetionable_path=quetionable_path
        self.save_path=save_path
        self.__result={}

    def validate(self):
        # os.path.basename(),os.path.split(),os.path.splitext()
        if os.path.exists(self.filepath) and (os.path.splitext(self.filepath))[1]=='.txt':      #验证文件是不是.TXT文件
            try:
                str_filepath = []
                str_filepath.extend(os.path.basename(self.filepath).strip().split('_'))
                date_text = str_filepath[4]                    # 判断时间的合法性和文件名字段个数是否符合
                if datetime.strptime(date_text, '%Y%m%d%H%M%S') and len(str_filepath) == 9:
                    return True
                else:
                    shutil.move(self.filepath, self.quetionable_path)
                    return False
            except ValueError:
                shutil.move(self.filepath, self.quetionable_path)
                return False
        else:
            shutil.move(self.filepath, self.quetionable_path)
            return False

    def parse(self):
      #if os.path.exists(self.filepath) and os.path.isfile(self.filepath):
        try:
            with open(self.filepath, 'r') as f:
                # 解析数据文件的全局配置参数
                # header = {'type':..., 'version':..., 'station_id':..., ... , 'alt':...}
                config_vals = f.readline().strip().split()
                config_vals.extend(f.readline().strip().split())
                config_keys = ('types', 'version', 'station_id', 'lon', 'lat', 'alt')
                header = {}
                for pair in zip(config_keys, config_vals):
                    if pair[0] in ('lon', 'lat', 'alt'):
                        header[pair[0]] = fast_float(pair[1], None)
                    else:
                        header[pair[0]] = pair[1]
                # 解析低、中、高三种模式的配置参数和数据,每一个模式的解析结果保存在一个bucket字典中
                # 三个模式的结果，保存于buckets中
                # bucket = {'config': [...], 'R': [[...],...,[...]], ...,'dataset':[[...]]}
                # buckets = { 'low': a bucket, 'middle': a bucket, 'high': a bucket}
                buckets = {}
                error_msg = ''
                for mode in ('low', 'middle', 'high'):
                    bucket = buckets.setdefault(mode, {})
                    config_vals = f.readline().strip().split()
                    config_vals.extend(f.readline().strip().split())
                    bin_num = int(config_vals[8])
                    bin_marks = config_vals[-5].replace('/', '')
                    if len(bin_marks) != bin_num:
                        error_msg = '波束个数和有效波束标识符个数不匹配'
                        break
                    bucket['config'] = config_vals
                    parsed_bins = 0
                    for row in f:
                        if row.strip().startswith('RAD') or not row.strip():
                            continue
                        elif row.strip().startswith('NNNN'):
                            parsed_bins += 1
                            # 如果当前模式下所有有效波束的数据全部解析完成 则保存解析结果，
                            # 跳转至下一个模式数据的解析
                            if parsed_bins == bin_num:
                                bucket['dataset']=list(map(lambda rows:reduce(lambda r1,r2:r1+r2[1:],rows),
                                                           zip(*[bucket[c] for c in bin_marks])))

                                break
                        else:
                            bin_rows = bucket.setdefault(bin_marks[parsed_bins], [])
                            # bin_rows.append([None if i.startswith('/') else float(i) for i in row.strip().split()])
                            bin_rows.append([fast_float(i, None) for i in row.strip().split()])
                if error_msg:
                    print(error_msg)
                    shutil.move(self.filepath, self.quetionable_path)
                else:
                    buckets['header']=header
                    self.__result=buckets
                    return True
                    # print(header)
                    # print(buckets['low'])
                    # print(buckets['middle'])
                    # print(buckets['high'])
        except Exception as e:
            shutil.move(self.filepath, self.quetionable_path)
            print("解析过程出现异常", e)
            return False

    def output_json(self):
        try:
            with open(os.path.join(self.save_path,os.path.splitext(self.filepath)[0]+'.json'), 'a') as f:
                json.dump(self.__result,f)
        except OSError:
            print('打开文件失败')

    def output_mysql(self):
        dt=self.__result
        id = dt['header']['station_id']
        conn = mysql.connector.connect(host="localhost", user='admin', password='123456',
                                       database='wndrad', charset="utf8")
        cur = conn.cursor()
        # 执行statio_basic_info，插入数据
        try:
            sql = "insert into statio_basic_info (station_id,radar_type,version,lon,lat,alt)" \
                  "values(%(station_id)s,%(types)s,%(version)s,%(lon)s,%(lat)s,%(alt)s)"
            cur.execute(sql, dt['header'])
        except Exception as e:
                if e=='MySQLInterfaceError':
                    pass
                else:
                    conn.rollback()
        # 执行wndrad_data，插入数据
        all_data = reduce(lambda r1,r2:r1+r2,[list(map(lambda body: [id, dt[mode]['config'][-12], mode] + body, dt[mode]['dataset']))
                                              for mode in ('low', 'middle', 'high')])
        try:
            sql = "insert into wndrad_data(station_id,sample_time,radar_mode,sample_height,r1,r2,r3,n1,n2,n3,e1,e2,e3,s1,s2,s3,w1,w2,w3)values" \
                  "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.executemany(sql, all_data)
        except Exception as e:
            if e == 'MySQLInterfaceError':
                pass
            else:
                conn.rollback()
        conn.commit()
        cur.close()
        conn.close()

p = Parser(r'C:\Users\Administrator\Desktop\all_file\save_path\Z_RADA_L_54399_20180708180241_O_WPRD_LC_RAD.txt',
           r'C:\Users\Administrator\Desktop\all_file\quetionable_path',
           r'C:\Users\Administrator\Desktop\all_file\save_path')

if p.validate():
    if p.parse():
        p.output_mysql()
        p.output_json()
