import time
import mysql.connector
import math
import threading
import os
import psutil
from datetime import datetime


class MysqlConnector:
    def __init__(self, host='localhost', user='root', passwd='root', name='sakila'):
        super().__init__()
        self.dbhost = host
        self.dbuser = user
        self.dbpasswd = passwd
        self.dbname = name
        self.conn = self.connect_db()
        if self.conn:
            self.cursor = self.conn.cursor()

    def connect_db(self):
        conn = mysql.connector.connect(host=self.dbhost, user=self.dbuser, passwd=self.dbpasswd, db=self.dbname)
        return conn

    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def fetch_results(self, sql, json=True):
        results = False
        if self.conn:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if json:
                columns = [col[0] for col in self.cursor.description]
                return [dict(zip(columns, row)) for row in results]
        return results

    def execute(self, sql):
        results = False
        if self.conn:
            self.cursor.execute(sql)

class MySQLEnv:
    def __init__(self, knobs_list, states_list, default_knobs):
        super().__init__()
        self.knobs_list = knobs_list
        self.states_list = states_list
        self.db_con = MysqlConnector()
        self.states = []
        self.score = 0.
        self.steps = 0
        self.terminate = False
        self.last_latency = 0
        self.default_latency = 0
        self.default_knobs = default_knobs

        self.pid = 9999
        self.pre_combine_log_file_size = 0
        self.mycnf = os.environ.get('MYCNF')

    def db_is_alive(self):
        flag = False
        while True:
            for proc in psutil.process_iter():
                if proc.name() == "mysqld.exe":
                    flag = True
                    break
            if flag:
                break
            time.sleep(20)
    ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
    def apply_knobs(self, knobs):
        self.db_is_alive()

        modify_concurrency = False
        if 'innodb_thread_concurrency' in knobs.keys() and knobs['innodb_thread_concurrency'] * (
                200 * 1024) > self.pre_combine_log_file_size:
            true_concurrency = knobs['innodb_thread_concurrency']
            modify_concurrency = True
            knobs['innodb_thread_concurrency'] = int(self.pre_combine_log_file_size / (200 * 1024.0)) - 2


        log_size = knobs['innodb_log_file_size'] if 'innodb_log_file_size' in knobs.keys() else 50331648
        log_num = knobs['innodb_log_files_in_group'] if 'innodb_log_files_in_group' in knobs.keys() else 2
        if 'innodb_thread_concurrency' in knobs.keys() and knobs['innodb_thread_concurrency'] * (200 * 1024) > log_num * log_size:
            return False

        knobs_rdsL = knobs
        try:
            RESTART_WAIT_TIME = 60
            time.sleep(RESTART_WAIT_TIME)

            db_conn = MysqlConnector(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     passwd=self.passwd,
                                     name=self.dbname,
                                     socket=self.sock)
            sql1 = 'SHOW VARIABLES LIKE "innodb_log_file_size";'
            sql2 = 'SHOW VARIABLES LIKE "innodb_log_files_in_group";'
            r1 = db_conn.fetch_results(sql1)
            file_size = r1[0]['Value'].strip()
            r2 = db_conn.fetch_results(sql2)
            file_num = r2[0]['Value'].strip()
            self.pre_combine_log_file_size = int(file_num) * int(file_size)

            if modify_concurrency:
                knobs_rdsL['innodb_thread_concurrency'] = true_concurrency
                knobs['innodb_thread_concurrency'] = true_concurrency

            if len(knobs_rdsL):
                tmp_rds = {}
                for knob_rds in knobs_rdsL:
                    tmp_rds[knob_rds] = knobs[knob_rds]

                db_conn = MysqlConnector(host=self.host,
                                         port=self.port,
                                         user=self.user,
                                         passwd=self.passwd,
                                         name=self.dbname,
                                         socket=self.sock)
                if 'innodb_io_capacity' in tmp_rds.keys():
                    tmp_rds['innodb_io_capacity'] = 2 * int(tmp_rds['innodb_io_capacity'])
                for k, v in tmp_rds:
                    sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(k)
                    r = db_conn.fetch_results(sql)
                    if r[0]['Value'] == v:
                        return True
                    sql = f"SET GLOBAL {k}={v}"
                    try:
                        db_conn.execute(sql)
                    except:
                        sql = f"SET {k}={v}"
                        db_conn.execute(sql)
                db_conn.close_db()
        except:
            return False
        return True

    ###

    def get_latency(self):
        t1 = float(datetime.utcnow().strftime('%S.%f'))
        self.db_con.execute("SELECT COUNT(*) FROM actor")
        t2 = float(datetime.utcnow().strftime('%S.%f'))
        return math.fabs(t2-t1)

    def get_internal_metrics(self, internal_metrics):
        global BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME, TIMEOUT, RESTART_FREQUENCY
        BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME, RESTART_FREQUENCY = 120, 30, 200
        TIMEOUT = BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME + 15

        self.connect_sucess = True
        _counter = 0
        _period = 5
        count = (BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME) / _period - 1
        warmup = BENCHMARK_WARMING_TIME / _period

        def collect_metric(counter):
            counter += 1
            print(counter)
            timer = threading.Timer(float(_period), collect_metric, (counter,))
            timer.start()
            if counter >= count:
                timer.cancel()
            try:
                db_conn = MysqlConnector(host=self.host,
                                         port=self.port,
                                         user=self.user,
                                         passwd=self.passwd,
                                         name=self.dbname,
                                         socket=self.sock)
            except:
                if counter > warmup:
                    self.connect_sucess = False
                    return
            try:
                if counter > warmup:
                    sql = 'SELECT NAME, COUNT from information_schema.INNODB_METRICS where status="enabled" ORDER BY NAME'
                    res = db_conn.fetch_results(sql, json=False)
                    res_dict = {}
                    for (k, v) in res:
                        res_dict[k] = v
                    internal_metrics.append(res_dict)
            except Exception:
                self.connect_sucess = False

        collect_metric(_counter)
        return internal_metrics

    def get_states(self):
        #internal_metrics = Manager().list()
        external_metrics = self.get_latency()
        internal_metrics = self.get_internal_metrics()
        return external_metrics, internal_metrics

    ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
    def step(self, knobs):
        self.steps += 1
        k_s = self.apply_knobs(knobs)
        s = self.get_states()
        latency, internal_metrics, resource = s
        reward = self.get_reward(latency)
        next_state = internal_metrics

        return next_state, reward, False, latency

    def init(self):
        self.score = 0.
        self.steps = 0
        self.terminate = False

        flag = self.apply_knobs(self.default_knobs)
        while not flag:
            print("Waiting 20 seconds. apply_knobs")
            time.sleep(20)
            flag = self.apply_knobs(self.default_knobs)

        s = self.get_states()
        while not s:
            print("Waiting 20 seconds. get_states")
            time.sleep(20)
            s = self.get_states()

        latency, internal_states, resource = s

        self.last_latency = latency
        self.default_latency = latency
        state = internal_states
        return state

    def get_reward(self, latency):

        def calculate_reward(delta0, deltat):
            if delta0 > 0:
                _reward = ((1 + delta0) ** 2 - 1) * math.fabs(1 + deltat)
            else:
                _reward = - ((1 - delta0) ** 2 - 1) * math.fabs(1 - deltat)

            if _reward > 0 and deltat < 0:
                _reward = 0
            return _reward

        if latency == 0:
            return 0

        delta_0_lat = float((-latency + self.default_latency)) / self.default_latency
        delta_t_lat = float((-latency + self.last_latency)) / self.last_latency
        reward = calculate_reward(delta_0_lat, delta_t_lat)

        self.score += reward

        return reward
