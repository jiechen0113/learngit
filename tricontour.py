# -*-coding: utf-8-*-
import json
import matplotlib.pyplot as plt
import time
import numpy as np
# 计算函数运行的时间
def timer(func):
    def wrapper(*args, **kwds):
        t0 = time.time()
        func(*args, **kwds)
        t1 = time.time()
        print('耗时%0.3f' % (t1 - t0,))
    return wrapper
@timer
def path(filename):
    try:
        fig, (ax2, ax3) = plt.subplots(nrows=2)
        data_list = []
        high_list = []
        time_list = []
        with open(filename, 'r') as f:
            cloud = f.read()
            cloud_dict = eval(cloud)            #将其转化为字典并进行提取（一般出现索引must be int,not str）
            cloud_data = cloud_dict["data"]
            [time_list.append(cloud_data[i][0])   for i in range(len(cloud_data))]
            [high_list.append(cloud_data[i][1])   for i in range(len(cloud_data))]
            [data_list.append(cloud_data[i][2])   for i in range(len(cloud_data))]

            z = np.array(data_list,dtype='float32')
            x = np.array(time_list,dtype='float32')
            y = np.array(high_list,dtype='float32')

            # Tricontour

            ax2.tricontour(x, y, z, levels=14, linewidths=0.5, colors='k')
            cntr2 = ax2.tricontourf(x, y, z, levels=14, cmap="RdBu_r")
            fig.colorbar(cntr2, ax=ax2)
            # ax2.plot(x, y, 'ko', ms=3)
            ax2.set(xlim=(time_list[0], time_list[len(time_list)- 1]), ylim=(0, 15000))
            ax2.set_title('tricontour')

            line = np.arange(-37.5, 40.1, 2.5)
            cs = ax3.tricontour(x, y, z, line,linewidths=1)
            plt.clabel(cs, fontsize=10, colors=('k', 'b', 'g'), fmt='%.1f')
            ax3.set(xlim=(time_list[0], time_list[len(time_list) - 1]), ylim=(0, 15000))
            # plt.clabel(cs, fontsize=10,colors=('k', 'r', 'g','w','y','b'),fmt='%.1f')
            ax3.set_title('tricontour lines -37.5 to 40')
            fig.colorbar(cntr2, ax=ax3)
            plt.subplots_adjust(hspace=0.5)
            # plt.savefig(r'C:\Users\Administrator\Desktop\1a.jpg')

            # 查看返回的ContourSet的collections属性。特别是第一个集合的get_paths（）方法返回组成每个线段的成对点
            coord_point = {}
            for value in line:
             try:
                bs = ax3.tricontour(x, y, z, [value], linewidths=1)
                p = bs.collections[0].get_paths()
                point= []     # 定义等值线坐标集合
                [point.extend(p[i].vertices) for i in range(len(p))]
                
                bin_rows = coord_point.setdefault(value, [])
                bin_rows.extend(np.array(point).tolist())

             except Exception as e:
                pass
                continue
            b = json.dumps(coord_point, default=lambda obj: obj.__dict__, ensure_ascii=False)
            with open(r'C:\Users\Administrator\Desktop\tri_cp.josn', 'a') as f:
                f.writelines(b + '\n')
            plt.show()
    except Exception as e:
        print(e)

filename=r'C:\Users\Administrator\Desktop\cloud_ref.json'
path(filename)