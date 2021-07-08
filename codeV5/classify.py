# -*- coding: utf-8 -*-
"""
Created on Thu Jul  1 21:04:37 2021

@author: Peikai_Li
"""


import time  
import pandas as pd
import os
import shutil
import re

import warnings
warnings.filterwarnings('ignore')

#import json

def contentParse(dict_like_str):
    # 先过滤字典格式数据,再过滤非字典格式数据
    try:
        dict_str = eval(dict_like_str)
        title = dict_str['Title']
        date = dict_str['PubDate'] 
        html = dict_str['Html']
        html = textParse(html)
        res = ''.join([title,date,html])
        return res
    except Exception:   # 万能异常,表示字符串不是字典格式
        return textParse(repr(dict_like_str))

def textParse(str_doc):
    # 正则过滤非字典格式数据
    #normal_pat =r'\s|\\n|&[A-Za-z0-9]+;|＆ｌｄｑｕｏ;|＆ｒｄｑｕｏ;' # 空格与\\n
    normal_pat =r'\s|\\n|&lt;|&gt;|＆ｌｄｑｕｏ;|＆ｒｄｑｕｏ;' # 空格与\\n
    unicode_pat = r'\\xa0|\\u3000|\\u0026#xA;' # 该死的unicode字符串
    html_pat = r'<([^>]*)>' # 去掉html标记
    extra_pat = r'\\' # 去掉\\
    # 注意顺序有讲究
    str_doc = re.sub(html_pat, '', str_doc)
    str_doc = re.sub(normal_pat, '', str_doc)
    str_doc = re.sub(unicode_pat, '', str_doc)
    str_doc = re.sub(extra_pat, '', str_doc)
    str_doc = str_doc.replace('\n', '').replace('&#xA;', '').replace('&amp;', '').replace('#xA;', '') #奇怪的&#xA,&amp,#xA
    
    return str_doc

def clean_data(data):
    """
    清洗数据
    :param data:原始脏数据
    :return: clean_df,dataframe格式
    """
    try:
        del data['Unnamed: 0']  # 无用列删除,防止错位(这一列只有部分csv有)
    except KeyError:
        pass
    
    data.rename(columns={'???"FFL_FJID"': 'FFL_FJID'}, inplace=True)  # 重命名第一列     
    
    data['FFL_CONTENT'] = data['FFL_CONTENT'].apply(lambda x: contentParse(x))  # 先字典过滤,再正常过滤
    data['FFL_CPCONTENT'] = data['FFL_CPCONTENT'].apply(lambda x: contentParse(x))  # 先字典过滤,再正常过滤
    
    return data


def filter_csv(csv_path, chunksize,typelist):
    """
    按指定条件读取单个csv中数据
    :param csv_path:单个csv文件路径
    :param chunksize:一次读取的行数
    :return: df,dataframe格式
    """
    reader = pd.read_csv(csv_path, encoding='utf-8', chunksize=chunksize)
    for chunk in reader:        
        for i in range(len(typelist)):
            chunk_type = pd.DataFrame()
            # i < len(typelist) means typelist[i] is exist and can match that case            
            if i < (len(typelist)-1):                
                chunk_type = chunk[chunk['FFL_CLASSCODE'] == typelist[i]]
            else:                 
                chunk_type = chunk[(~chunk['FFL_CLASSCODE'].isin(typelist[:-1]))|(chunk['FFL_CLASSCODE'].isna())] 
                #筛选出不在typelist里面或者缺失值的
            print(f'{typelist[i]}{len(chunk_type)}')
            #starat clean                
            chunk_type = clean_data(chunk_type)
            
            
            # 对地区['FFL_AREACODE']的异常值集体写入
            # 先对地址编码有错误或缺失的统一写入
            area_temp_df = pd.DataFrame() #初始化
            area_temp_df = chunk_type[(~chunk_type['FFL_AREACODE'].isin(list(range(110000,660000))))|
                                 (chunk_type['FFL_AREACODE'].isna())] 
            # ~ 表示否定， isin后面加list
            if len(area_temp_df) != 0:
                # 指定地点的文件路径
                area_csv_path = os.path.join('result', typelist[i],
                                                typelist[i] + 'areaoutrange' + '.csv')#后期可以放到一个文件夹里面
                # 判断文件是否存在,存在则不写header
                if os.path.exists(area_csv_path):
                    area_temp_df.to_csv(area_csv_path,
                                      mode="a",
                                      index=False,
                                      header=False,
                                      encoding='utf-8')
                else:
                    area_temp_df.to_csv(area_csv_path,
                                      mode="a",
                                      index=False,
                                      header=True,
                                      encoding='utf-8')                       
            
            # 对地区['FFL_AREACODE']的正常值集体写入
            for num in range(11, 66):
                # 处理民事案件,这样是因为两位数编号确定一个地点,且两个判断条件可以合起来写,注意使用//整除
                area_temp_df = pd.DataFrame() #初始化
                area_temp_df = chunk_type[chunk_type['FFL_AREACODE'] // 10000 == num]
                #目前发现['FFL_AREACODE']数据较为规范，未作处理，直接对其他类型的数据做整除会报错！
                #df.query("JS==24 or PY ==33")
                # 如果没有该地点案件则继续
                if len(area_temp_df) == 0:
                    continue    
                
                # 对判决时间['FFL_JUDGDATE"]的异常值集体写入和2012之前的案件
                temp_df = pd.DataFrame() #初始化
                temp_df = area_temp_df[(area_temp_df["FFL_JUDGDATE"]<="2012-12-31")|
                                 (area_temp_df['FFL_JUDGDATE'].isna())] 
                # 指定地点的文件路径  
                area_csv_path = os.path.join('result', typelist[i],
                                                typelist[i] + str(num)+'_2012_other'+ '.csv')
                # 判断文件是否存在,存在则不写header
                if os.path.exists(area_csv_path):
                    temp_df.to_csv(area_csv_path,
                                      mode="a",
                                      index=False,
                                      header=False,
                                      encoding='utf-8')
                else:
                    temp_df.to_csv(area_csv_path,
                                      mode="a",
                                      index=False,
                                      header=True,
                                      encoding='utf-8')                
                
                
                for timeyear in range(2013,2020):
                    temp_df = pd.DataFrame() #初始化
                    temp_df = area_temp_df[(area_temp_df["FFL_JUDGDATE"]<=str(timeyear)+"-12-31")&
                                  (area_temp_df["FFL_JUDGDATE"]>str(timeyear-1)+"-12-31")]                    
                    # 指定地点的文件路径  
                    area_csv_path = os.path.join('result', typelist[i],
                                                    typelist[i] + str(num)+'_'+str(timeyear) + '.csv')
                    # 判断文件是否存在,存在则不写header
                    if os.path.exists(area_csv_path):
                        temp_df.to_csv(area_csv_path,
                                          mode="a",
                                          index=False,
                                          header=False,
                                          encoding='utf-8')
                    else:
                        temp_df.to_csv(area_csv_path,
                                          mode="a",
                                          index=False,
                                          header=True,
                                          encoding='utf-8')
            


def main():
    
    start = time.process_time()
    
    file_dir = 'alldata/'  # 这里指定存放csv的文件夹
    chunksize = 25100  # 这里指定一次读取的小批量数据行数
    fileArray = []  # 预分配 存储所有csv文件名的空间
    for root, dirs, files in os.walk(file_dir):
        for fn in files:
            each_path = os.path.join(root, fn)
            fileArray.append(each_path)
            
    typelist=["ms","pc","xz","xs","zscq","zx","gx","pcjz","others_nan"]
    # 民事，赔偿案件，行政，刑事，知识产权,执行,管辖,赔偿司法救助,(未分类)
    #typelist=["ms","xz"]
    list_dir = [] # 预分配  
    for i in range(len(typelist)): 
        #print(i)
        list_dir.append(os.path.join('result', typelist[i]))      
        if os.path.exists(list_dir[i]):
            shutil.rmtree(list_dir[i])
        os.makedirs(list_dir[i])
    
      
    for file in fileArray:
        print(f'处理{file}')
        filter_csv(file, chunksize,typelist)  # 一个csv的数据
        print(f'{file}处理完毕')
        
    end= time.process_time()
    print('Running time: %s Seconds'%(end-start))

if __name__ == '__main__':
    main()
