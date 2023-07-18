import time
import mysql.connector
import math
from .utils.parser import parse_tpcc, parse_sysbench, parse_oltpbench, parse_cloudbench, ConfigParser
import threading
from subprocess import Popen, TimeoutExpired, STDOUT, PIPE
from multiprocessing import Manager, Value, Process
import os
import psutil
import numpy as np
from .utils.resource_monitor import ResourceMonitor
from knobs_definition import value_type_metrics


im_alive = Value('b', False)
CPU_CORE = 8

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

    ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
    def apply_knobs(self, knobs):
        # self.db_con.connect_db()
        # res = False
        # return res

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

        cnf_parser = ConfigParser(self.mycnf)
        konbs_not_in_mycnf = []
        for key in knobs.keys():
            if not key in self.knobs_detail.keys():
                konbs_not_in_mycnf.append(key)
                continue
            cnf_parser.set(key, knobs[key])
        cnf_parser.replace()
        knobs_rdsL = konbs_not_in_mycnf

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
            if len(knobs_rdsL):
                tmp_rds = {}
                for knob_rds in knobs_rdsL:
                    tmp_rds[knob_rds] = knobs[knob_rds]
                self.apply_rds_knobs(tmp_rds)
            if modify_concurrency:
                tmp = {}
                tmp['innodb_thread_concurrency'] = true_concurrency
                self.apply_rds_knobs(tmp)
                knobs['innodb_thread_concurrency'] = true_concurrency
        except:
            return False
        return True

    def apply_rds_knobs(self, knobs):
        db_conn = MysqlConnector(host=self.host,
                                 port=self.port,
                                 user=self.user,
                                 passwd=self.passwd,
                                 name=self.dbname,
                                 socket=self.sock)
        if 'innodb_io_capacity' in knobs.keys():
            self.set_rds_param(db_conn, 'innodb_io_capacity_max', 2 * int(knobs['innodb_io_capacity']))
        for key in knobs.keys():
            self.set_rds_param(db_conn, key, knobs[key])
        db_conn.close_db()
        return True

    ###

    def get_external_metrics(self, filename=''):
        result = ''
        if self.workload['name'] == 'tpcc':
            result = parse_tpcc(filename)
        elif self.workload['name'] == 'tpcc_rds':
            result = parse_tpcc(filename)
        elif self.workload['name'] == 'sysbench':
            result = parse_sysbench(filename)
        elif self.workload['name'] == 'sysbench_rds':
            result = parse_sysbench(filename)
        elif self.workload['name'] == 'oltpbench':
            result = parse_oltpbench('results/{}.summary'.format(filename))

        else:
            result = parse_cloudbench(filename)
        return result

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
            if counter >= count or not im_alive.value:
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
            except Exception as err:
                self.connect_sucess = False


        collect_metric(_counter)
        return internal_metrics

    def get_states(self, collect_cpu=0):
        start = time.time()
        self.connect_sucess = True
        p = psutil.Process(self.pid)
        if len(p.cpu_affinity()) != CPU_CORE:
            command = 'sudo cgclassify -g memory,cpuset:sever ' + str(self.pid)
            os.system(command)

        internal_metrics = Manager().list()
        im = Process(target=self.get_internal_metrics, args=(internal_metrics,))
        im_alive.value = True
        im.start()
        if collect_cpu:
            rm = ResourceMonitor(self.pid, 1, BENCHMARK_WARMING_TIME, BENCHMARK_RUNNING_TIME)
            rm.run()



        ####!  get_benchmark_cmd()
        # cmd, filename = self.get_benchmark_cmd()
        # v = p.cpu_percent()
        # print("[{}] benchmark start!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        # p_benchmark = Popen(cmd, shell=True, stderr=STDOUT, stdout=PIPE, close_fds=True)
        # try:
        #     outs, errs = p_benchmark.communicate(timeout=TIMEOUT)
        #     ret_code = p_benchmark.poll()
        #     if ret_code == 0:
        #         print("[{}] benchmark finished!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        # except TimeoutExpired:
        #     print("[{}] benchmark timeout!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        # clear_cmd = """mysqladmin processlist -uroot -S$MYSQL_SOCK | awk '$2 ~ /^[0-9]/ {print "KILL "$2";"}' | mysql -uroot -S$MYSQL_SOCK """
        # Popen(clear_cmd, shell=True, stderr=STDOUT, stdout=PIPE, close_fds=True)
        # print("[{}] clear processlist".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        # im_alive.value = False
        # im.join()
        # if collect_cpu:
        #     rm.terminate()
        #
        # if not self.connect_sucess:
        #
        #     return None
        # external_metrics = self.get_external_metrics(filename)
        # internal_metrics, dirty_pages, hit_ratio, page_data = self._post_handle(internal_metrics)
        #
        # if collect_cpu:
        #     monitor_data_dict = rm.get_monitor_data()
        #     interval = time.time() - start
        #     avg_read_io = sum(monitor_data_dict['io_read']) / (len(monitor_data_dict['io_read']) + 1e-9)
        #     avg_write_io = sum(monitor_data_dict['io_write']) / (len(monitor_data_dict['io_write']) + 1e-9)
        #     avg_virtual_memory = sum(monitor_data_dict['mem_virtual']) / (len(monitor_data_dict['mem_virtual']) + 1e-9)
        #     avg_physical_memory = sum(monitor_data_dict['mem_physical']) / (
        #                 len(monitor_data_dict['mem_physical']) + 1e-9)
        #     resource = (None, avg_read_io, avg_write_io, avg_virtual_memory, avg_physical_memory, dirty_pages, hit_ratio, page_data)
        #
        #     return external_metrics, internal_metrics, resource
        # else:
        #     return external_metrics, internal_metrics, [0]*8


    def _post_handle(self, metrics):
        result = np.zeros(65)

        def do(metric_name, metric_values):
            metric_type = 'counter'
            if metric_name in value_type_metrics:
                metric_type = 'value'
            if metric_type == 'counter':
                return float(metric_values[-1] - metric_values[0]) * 23 / len(metric_values)
            else:
                return float(sum(metric_values)) / len(metric_values)

        keys = list(metrics[0].keys())
        keys.sort()
        total_pages = 0
        dirty_pages = 0
        request = 0
        reads = 0
        page_data = 0
        page_size = 0
        page_misc = 0
        for idx in range(len(keys)):
            key = keys[idx]
            data = [x[key] for x in metrics]
            result[idx] = do(key, data)
            if key == 'buffer_pool_pages_total':
                total_pages = result[idx]
            elif key == 'buffer_pool_pages_dirty':
                dirty_pages = result[idx]
            elif key == 'buffer_pool_read_requests':
                request = result[idx]
            elif key == 'buffer_pool_reads':
                reads = result[idx]
            elif key == 'buffer_pool_pages_data':
                page_data = result[idx]
            elif key == 'innodb_page_size':
                page_size = result[idx]
            elif key == 'buffer_pool_pages_misc':
                page_misc = result[idx]
        dirty_pages_per = dirty_pages / total_pages
        hit_ratio = request / float(request + reads)
        page_data = (page_data + page_misc) * page_size / (1024.0 * 1024.0 * 1024.0)

        return result, dirty_pages_per, hit_ratio, page_data


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
