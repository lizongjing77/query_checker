#!pip install --upgrade google-cloud-bigquery six==1.14.0
import numpy as np
import pandas as pd
import os.path
from os import path
import re
from collections import OrderedDict
import logging
import time
from google.cloud import bigquery
from google.oauth2 import service_account

class SQL_checker:
    
    def __init__(self,query_path,mode,json_path,project_id):
        if isinstance(query_path,str):
            path = query_path
        self.query_path = path
        self.mode = mode
        self.json_path = json_path
        self.project_id = project_id
        self.logging()
        self.describe_list = ['PK' ,'MAX', 'MIN', 'AVG',
                              'MODE', 'UNIQ_VAL', 'NULLFRAC', 
                              'NULLCNT']
        
        self.with_clause_string = self.read_sql()
        self.table_loc = self.get_with_clauses()
        self.table_list = list(self.get_with_clauses().keys())
        self.clause_dict = self.with_clause()
        self.table_dict = self.find_describe()
        
        self.install_bg()
        self.write_sql_file()
        self.write_results()
        
    def logging(self):
        
        t = time.localtime()
        current_time = time.strftime("%Y%m%d%H%M%S", t)            
        filename = f'./log/qc_log_{current_time}.log'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise        
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)            
        logging.basicConfig(filename=filename,format='%(asctime)s %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)
        
    #クエリを読み込む
    def read_sql(self):
        if path.exists(self.query_path):
            logging.debug("sqlファイルを読み込み開始：")
            with open(self.query_path,encoding="utf-8_sig",errors='ignore') as sql_txt:
                copy = False    
                with_clause_string = """ """
                for line in sql_txt:
                    #with句開始コメントを発見したら、コピーを始める
                    if line.strip() == "/*with clauses start*/":
                        copy = True
                        continue
                    #with句終了コメントを発見したら、コピーを止める
                    elif line.strip() == "/*with clauses end*/":
                        copy = False
                        continue
                    elif copy:
                        with_clause_string += line
            logging.debug("読み込み完了")          
        else:
            logging.debug('パスあるいはファイルは存在しない。')
        return with_clause_string
    
    def read_whole_sql(self):
        #
        if path.exists(self.query_path):
            with open(query_path,encoding="utf-8_sig",errors='ignore') as sql_txt:
                with_clause_string = """ """
                for line in sql_txt:
                    with_clause_string += line
        else:
            logging.debug('パスあるいはファイルは存在しない。')
        return with_clause_string
    
    #各with句のクエリを辞書型に収納
    def with_clause(self):
        logging.debug('with句クエリを抽出。')
        clause_dict = {}
        for table,loc in self.table_loc.items():
            with_clause = self.read_whole_sql()[loc[0]:loc[1]]
            clause_dict[table] = with_clause
        logging.debug('with句クエリ抽出完了。')
        return clause_dict  
    
    def convert_text(self,text, ran, to=None):
        if to is None:
            to = " " * (ran[1] - ran[0])
        else:
            assert len(to) == ran[1] - ran[0]
        text_new = text[:ran[0]] + to + text[ran[1]:]
        return text_new    
    
    def get_with_clauses(self):
        logging.debug('with句テーブル名を抽出。')
        with_clause_string_new = self.read_whole_sql()
        
        # /* */ 式のコメントを見つけて、空白に置換する
        comment_all = list(re.finditer(r'/\*[\s\S]*?\*/', self.read_whole_sql()))
        if len(comment_all) > 0:
            for rr in comment_all:
                with_clause_string_new = self.convert_text(with_clause_string_new, rr.span())    
        
        # そもそも with があるのか
        res = re.findall(r'[Ww][Ii][Tt][Hh]', with_clause_string_new)
        if len(res) == 0:
            return None
        
        # WITH の終わりの ")" を探す
        # ")" の後に "," が入らずに "SELECT" が来る
        result = list(re.finditer(r'\)[^,]*?[Ss][Ee][Ll][Ee][Cc][Tt]', with_clause_string_new))
        with_end = result[0].span()[0]
        
        # 一番外側の "()" を探す
        stack = []
        with_clauses_pos = []  # with句のSELECT文の位置を格納
        table_name_cand_last = []
        for i, c in enumerate(with_clause_string_new):
            if c == '(':
                if i > with_end:
                    # もう with から抜けている
                    break
                stack.append(i)
            elif c == ')' and stack:
                start = stack.pop()
                if len(stack) == 0:
                    # 一番外側
                    with_clauses_pos.append((start+1, i))
    
        # "()" に入っていない部分
        table_name_cand = [(0, with_clauses_pos[0][0])]
        for j in range(len(with_clauses_pos)-1):
            table_name_cand.append((with_clauses_pos[j][1], with_clauses_pos[j+1][0]))
            
        # テーブル名と位置を辞書で格納
        with_clauses = OrderedDict()
        for j in range(len(table_name_cand)):
            serach_range = with_clause_string_new[table_name_cand[j][0]:table_name_cand[j][1]+1]
            # AS の直前の文字列を取得
            result = list(re.finditer(r'[Ss][Aa]\s+\S+?\s', serach_range[::-1]))
            result_span = result[0].span()
            table_name = serach_range[-result_span[1]:-result_span[0]-3].replace(' ', '')
            with_clauses[table_name] = with_clauses_pos[j]
        logging.debug('with句テーブル名抽出完了。')
        return with_clauses

    #各with句で、ほしい処理とほしいカラムを辞書型に返す
    #コメントが書いてあるテーブルを返す
    def find_describe(self):
        logging.debug('with句テーブルごとにカラムサマリを抽出。')
        table_dict = {}
        query_dict = {}
        not_emp_table_dict = {}
        for table,txt in self.clause_dict.items():
            table_describe_dict = {}
            for act in self.describe_list:
                clause_list = re.findall(f'((.+?)/\*###QC_{act}\*/)', txt)
                for i in range(len(clause_list)):
                    clause_list[i] = min(clause_list[i],key=len)    
                root_list = []
                for des in clause_list:
                    des = des.replace(',', '',1).split(' ')[-1]
                    if len(des.split(".")) != 1:
                        des_split = des.split(".")[-1].split('/*')[0]
                        root_list.append(des_split)
                    else:
                        root_list.append(des.split('/*')[0])
                table_describe_dict[act] = root_list
            table_dict[table] = table_describe_dict

        for table in self.table_list:
            if any(table_dict[table].values()):
                not_emp_table_dict[table] = {}
                for key,act in table_dict[table].items():
                    if len(act) != 0:
                        not_emp_table_dict[table][key] = act
        logging.debug('with句テーブルごとにカラムサマリ抽出完了。')                
        return not_emp_table_dict
    
    def get_unique_col_dict(self):
        unique_col_dict = {}
        for table in self.table_list:
            l=[]
            for values in self.table_dict[table].values():
                for val in values:
                    l.append(val)
            unique_col_dict[table] = np.unique(np.asarray(l))
        return unique_col_dict    
    
    #チェックするためのクエリを生成する
    def describe_query_writer(self):
        logging.debug('with句テーブルごとにカラムサマリチェッククエリを生成。')
        table_act_dict = {}
        for table,dict in self.table_dict.items():
            act_query = ''' '''
            table_name = f'{table}'
            from_clause = f'\nFROM {table}'
            non_pk_clause = f'SELECT\n\'{table_name}\' AS table_name\n, count(*) AS record_number\n' 
            for key,col in dict.items():
                if key == 'PK':
                    case_when_clause = ''
                    case_when_name = ''
                    group_by_clause = ''
                    for pk in col:
                        case_when_clause += f', CASE WHEN {pk} is null THEN 1 ELSE 0 END AS {pk}_null_f\n'
                        case_when_name += f', max({pk}_null_f) as {pk}_null_f\n'
                        group_by_clause += f'{pk},'
                    group_by_clause = group_by_clause[:-1]
                    pk_clause = f'SELECT\nmax(uni_f) as uni_f\n{case_when_name}FROM\n(\nSELECT\nCASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS uni_f\n{case_when_clause}FROM\n {table}\n GROUP BY\n {group_by_clause}\n)'
                else:
                    if key == 'MAX':
                        for c in col:
                            act_query += f', max({c}) as {key}_{c}\n'
                    elif key == 'MIN':
                        for c in col:
                            act_query += f', min({c}) as {key}_{c}\n'
                    elif key == 'AVG':
                        for c in col:
                            act_query += f', avg({c}) as {key}_{c}\n'
                    elif key == 'UNIQ_VAL':
                        for c in col:
                            act_query += f', count(distinct({c})) as {key}_{c}\n'
                    elif key == 'NULLFRAC':
                        for c in col:
                            act_query += f', 1 - (count({c})/count(*)) as {key}_{c}\n'  
                    elif key == 'NULLCNT':
                        for c in col:
                            act_query += f', count(*) - count({c}) as {key}_{c}\n'
            non_pk_clause += act_query + from_clause
            if 'PK' in dict.keys():
                select_clause = f'SELECT\n*\nFROM\n({non_pk_clause}) as npk\nleft outer join\n({pk_clause}) as pk\non 1=1'
            else:
                select_clause = f'SELECT\n*\nFROM\n({non_pk_clause})'
            table_act_dict[table] = select_clause
        logging.debug('with句テーブルごとにカラムサマリクエリ生成完了。')    
        return table_act_dict
    
    #sqlファイルを出力
    def write_sql_file(self):
        logging.debug('with句テーブルごとのチェッククエリをファイルに作成。')
        i = 0
        for table,query in self.describe_query_writer().items():
            logging.debug(f'{table}チェッククエリ 出力。')
            i += 1
            t = time.localtime()
            current_time = time.strftime("%Y%m%d%H%M%S", t)
            filename = f'./sql_query/summary_cheker_{i}_{table}_{current_time}.sql'
            if not os.path.exists(os.path.dirname(filename)):
                try:
                    os.makedirs(os.path.dirname(filename))
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise
            with open(filename, "w",encoding="utf-8_sig",errors='ignore') as f:
                f.write(query)
        logging.debug('with句テーブルごとのチェッククエリをファイルに作成完了。')
        return self
    
    #DBサーバーから帰ってくる結果を認識する
    def install_bg(self):
        
        logging.debug('以下のコマンドを使って、big query接続用のライブラリーをインストール。')
        print('以下のコマンドを使ってインストールをしてください')
        print('!pip install --upgrade google-cloud-bigquery')
        print('from google.cloud import bigquery')
        print('from google.oauth2 import service_account')
        
    def connect_bg(self):
    
        logging.debug('big queryに接続。')
        credentials = service_account.Credentials.from_service_account_file(self.json_path)
        client = bigquery.Client(credentials=credentials,project=self.project_id)
        logging.debug('big queryに接続成功。')
    
        return client
    
    def run_query(self):
        results = {}
        client = self.connect_bg()
        for table,query in self.describe_query_writer().items():
            logging.debug(f'{table}チェッククエリ 実行中。')
            if mode == 'standard':
                query_job = client.query(query).to_dataframe()
            elif mode == 'legacy':
                job_config.use_legacy_sql = True
                query_job = client.query(query).to_dataframe()

            results[table] = results
        return results
    
    def modify_output(self):
        
        logging.debug('チェッククエリ結果をに整理。')
        output = ''' '''
        for table,df in self.run_query().items():
            output += f'table:{table}\n'
            output += f'number of records: {df[df.columns[1]].values[0]}'
            a_list = []
            for col in df.columns[2:]:
                c = col.split('_',1)[0]
                if c in self.describe_list:
                    a_list.append(c)
            len_de = len(a_list)+2
            if len_de < len(df.columns):
                output +='PK check:\n'
                if df[df.columns[len_de]].values[0] == 0:
                    output += '  pk unique: yes\n'
                else: output += '  pk unique: no\n'
                if np.sum(df[df.columns[len_de+1:]].values[0]) == 0:
                    output += '  pk no null: yes\n'
                else: output += '  pk no null: no\n'
            for d in self.get_unique_col_dict()[table]:
                output += f'col:{d}\n'
                for col in df.columns[2:len_de]:
                    split = col.split('_',1)
                    if d in col:
                        output += f'  {split[0]}:{df[col].values[0]}\n'
            output += '------------------------------\n'
        logging.debug('チェッククエリ結果をに整理完了。')
        
        print(output)
        return output
    
    def write_results(self):
        logging.debug('チェッククエリ結果をファイルに作成。')
        results = ''' '''
        t = time.localtime()    
        current_time = time.strftime("%Y%m%d%H%M%S", t)
        filename = f'cheker_result_{current_time}.txt'
        with open(filename, "w",encoding="utf-8_sig",errors='ignore') as f:
            f.write(self.modify_output())
        logging.debug('チェッククエリ結果をファイルに作成完了。')
        return self