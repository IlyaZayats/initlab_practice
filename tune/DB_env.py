import os
import psutil
import time
import threading
import subprocess
import multiprocessing as mp
import numpy as np
from multiprocessing import Manager
from abc import ABC, abstractmethod
from .knobs import logger, gen_continuous, initialize_knobs, get_default_knobs
from .utils.parser import parse_tpcc, parse_sysbench, parse_oltpbench, parse_cloudbench, parse_job, ConfigParser
from .resource_monitor import ResourceMonitor
from .dbconnector import MysqlConnector


im_alive = mp.Value('b', False)
CPU_CORE = 8
TIMEOUT = 180
TIMEOUT_CLOSE = 90
RESTART_FREQUENCY = 20
RESTART_WAIT_TIME = 60

value_type_metrics = [
    'lock_deadlocks', 'lock_timeouts', 'lock_row_lock_time_max',
    'lock_row_lock_time_avg', 'buffer_pool_size', 'buffer_pool_pages_total',
    'buffer_pool_pages_misc', 'buffer_pool_pages_data', 'buffer_pool_bytes_data',
    'buffer_pool_pages_dirty', 'buffer_pool_bytes_dirty', 'buffer_pool_pages_free',
    'trx_rseg_history_len', 'file_num_open_files', 'innodb_page_size']


log_num_default = 2
log_size_default = 50331648


def generate_knobs(action, method):
    if method in ['ddpg']:
        return gen_continuous(action)
    else:
        raise NotImplementedError("Not implemented generate_knobs")


class DBEnv(ABC):
    def __init__(self, workload):
        self.score = 0.
        self.steps = 0
        self.terminate = False
        self.workload = workload

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def step(self, knobs, episode, step):
        pass

    @abstractmethod
    def terminate(self):
        return False


class MySQLEnv(DBEnv):
    def __init__(self,
                 workload,
                 knobs_config,
                 num_metrics,
                 log_path='',
                 threads=8,
                 host='localhost',
                 port=3392,
                 user='root',
                 passwd='',
                 dbname='tpcc',
                 sock='',
                 rds_mode=False,
                 workload_zoo_config='',
                 workload_zoo_app='',
                 oltpbench_config_xml='',
                 disk_name='nvme1n1',
                 tps_constraint=0,
                 latency_constraint=0,
                 pid=9999,
                 knob_num=-1,
                 y_variable='tps',
                 lhs_log='output.res'
                 ):
        super().__init__(workload)
        self.knobs_config = knobs_config
        self.mysqld = os.environ.get('MYSQLD')
        self.mycnf = os.environ.get('MYCNF')
        if not self.mysqld:
            logger.error('You should set MYSQLD env var before running the code.')
        if not self.mycnf:
            logger.error('You should set MYCNF env var before running the code.')
        self.workload = workload
        self.log_path = log_path
        self.num_metrics = num_metrics
        self.external_metricsdefault_ = []
        self.last_external_metrics = []
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.sock = sock
        self.threads = threads
        self.best_result = './autotune_best.res'
        self.knobs_config = knobs_config
        self.knobs_detail = initialize_knobs(knobs_config, knob_num)
        self.default_knobs = get_default_knobs()
        self.rds_mode = rds_mode
        self.oltpbench_config_xml = oltpbench_config_xml
        self.step_count = 0
        self.disk_name = disk_name
        self.workload_zoo_config = workload_zoo_config
        self.workload_zoo_app = workload_zoo_app
        self.tps_constraint = tps_constraint
        self.latency_constraint = latency_constraint
        self.pre_combine_log_file_size = 0
        self.connect_sucess = True
        self.pid = pid
        self.reinit_interval = 0
        self.reinit = True
        if self.rds_mode:
            self.reinit = False
        self.generate_time()
        self.y_variable = y_variable
        self.lhs_log = lhs_log

    def generate_time(self):
        global BENCHMARK_RUNNING_TIME
        global BENCHMARK_WARMING_TIME
        global TIMEOUT
        global RESTART_FREQUENCY

        if self.workload['name'] == 'sysbench' or self.workload['name'] == 'oltpbench':
            BENCHMARK_RUNNING_TIME = 120
            BENCHMARK_WARMING_TIME = 30
            TIMEOUT = BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME + 15
            RESTART_FREQUENCY = 200
        if self.workload['name'] == 'job':
            BENCHMARK_RUNNING_TIME = 240
            BENCHMARK_WARMING_TIME = 0
            TIMEOUT = BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME
            RESTART_FREQUENCY = 30000

    def apply_knobs(self, knobs):
        self._kill_mysqld()
        modify_concurrency = False
        if 'innodb_thread_concurrency' in knobs.keys() and knobs['innodb_thread_concurrency'] * (200 * 1024) > self.pre_combine_log_file_size:
            true_concurrency = knobs['innodb_thread_concurrency']
            modify_concurrency = True
            knobs['innodb_thread_concurrency'] = int(self.pre_combine_log_file_size / (200 * 1024.0)) - 2
            logger.info("modify innodb_thread_concurrency")

        if 'innodb_log_file_size' in knobs.keys():
            log_size = knobs['innodb_log_file_size']
        else:
            log_size = log_size_default
        if 'innodb_log_files_in_group' in knobs.keys():
            log_num = knobs['innodb_log_files_in_group']
        else:
            log_num = log_num_default

        if 'innodb_thread_concurrency' in knobs.keys() and knobs['innodb_thread_concurrency'] * (200 * 1024) > log_num * log_size:
            logger.info("innodb_thread_concurrency is set too large")
            return False

        knobs_rdsL = self._gen_config_file(knobs)
        sucess = self._start_mysqld()
        try:
            logger.info('sleeping for {} seconds after restarting mysql'.format(RESTART_WAIT_TIME))
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
            if len(knobs_rdsL) > 0:
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
            sucess = False

        return sucess

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
        elif self.workload['name'] == 'job':
            dirname, _ = os.path.split(os.path.abspath(__file__))
            select_file = dirname + '/cli/selectedList.txt'
            result = parse_job(filename, select_file)
        else:
            result = parse_cloudbench(filename)
        return result

    def get_internal_metrics(self, internal_metrics):
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
                    logger.info("connection failed during internal metrics collection")
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
                logger.info("connection failed during internal metrics collection")
                logger.info(err)

        collect_metric(_counter)
        return internal_metrics

    def get_states(self, collect_cpu=0):
        start = time.time()
        self.connect_sucess = True
        p = psutil.Process(self.pid)
        if len(p.cpu_affinity())!= CPU_CORE:
            command = 'sudo cgclassify -g memory,cpuset:sever ' + str(self.pid)
            os.system(command)

        internal_metrics = Manager().list()
        im = mp.Process(target=self.get_internal_metrics, args=(internal_metrics,))
        im_alive.value = True
        im.start()
        if collect_cpu:
            rm = ResourceMonitor(self.pid, 1, BENCHMARK_WARMING_TIME, BENCHMARK_RUNNING_TIME)
            rm.run()
        cmd, filename = self.get_benchmark_cmd()
        v = p.cpu_percent()
        print("[{}] benchmark start!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        p_benchmark = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
        try:
            outs, errs = p_benchmark.communicate(timeout=TIMEOUT)
            ret_code = p_benchmark.poll()
            if ret_code == 0:
                print("[{}] benchmark finished!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        except subprocess.TimeoutExpired:
            print("[{}] benchmark timeout!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        clear_cmd = """mysqladmin processlist -uroot -S$MYSQL_SOCK | awk '$2 ~ /^[0-9]/ {print "KILL "$2";"}' | mysql -uroot -S$MYSQL_SOCK """
        subprocess.Popen(clear_cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
        print("[{}] clear processlist".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        im_alive.value = False
        im.join()
        if collect_cpu:
            rm.terminate()

        if not self.connect_sucess:
            logger.info("connection failed")
            return None
        external_metrics = self.get_external_metrics(filename)
        internal_metrics, dirty_pages, hit_ratio, page_data = self._post_handle(internal_metrics)
        logger.info('internal metrics: {}.'.format(list(internal_metrics)))
        if collect_cpu:
            monitor_data_dict = rm.get_monitor_data()
            interval = time.time() - start
            avg_read_io = sum(monitor_data_dict['io_read']) / (len(monitor_data_dict['io_read']) + 1e-9)
            avg_write_io = sum(monitor_data_dict['io_write']) / (len(monitor_data_dict['io_write']) + 1e-9)
            avg_virtual_memory = sum(monitor_data_dict['mem_virtual']) / (len(monitor_data_dict['mem_virtual']) + 1e-9)
            avg_physical_memory = sum(monitor_data_dict['mem_physical']) / (
                        len(monitor_data_dict['mem_physical']) + 1e-9)
            resource = (None, avg_read_io, avg_write_io, avg_virtual_memory, avg_physical_memory, dirty_pages, hit_ratio, page_data)
            logger.info(external_metrics)
            return external_metrics, internal_metrics, resource
        else:
            return external_metrics, internal_metrics, [0]*8


    def _kill_mysqld(self):
        mysqladmin = os.path.dirname(self.mysqld) + '/mysqladmin'
        cmd = '{} -u{} -S {} shutdown'.format(mysqladmin, self.user, self.sock)
        p_close = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                                       close_fds=True)
        try:
            outs, errs = p_close.communicate(timeout=TIMEOUT_CLOSE)
            ret_code = p_close.poll()
            if ret_code == 0:
                print("Close database successfully")
        except subprocess.TimeoutExpired:
            print("Force close!")
            os.system("ps aux|grep '" + self.sock + "'|awk '{print $2}'|xargs kill -9")
            os.system("ps aux|grep '" + self.mycnf + "'|awk '{print $2}'|xargs kill -9")
        logger.info('mysql is shut down')

    def _start_mysqld(self):
        proc = subprocess.Popen([self.mysqld, '--defaults-file={}'.format(self.mycnf)])
        self.pid = proc.pid
        command = 'sudo cgclassify -g memory,cpuset:sever ' + str(self.pid)
        p = os.system(command)
        if not p:
            logger.info('add {} to memory,cpuset:sever'.format(self.pid))
        else:
            logger.info('Failed: add {} to memory,cpuset:sever'.format(self.pid))
        count = 0
        start_sucess = True
        logger.info('wait for connection')
        while True:
            try:

                dbc = MysqlConnector(host=self.host,
                                         port=self.port,
                                         user=self.user,
                                         passwd=self.passwd,
                                         name=self.dbname,
                                         socket=self.sock)
                db_conn = dbc.conn
                if db_conn.is_connected():
                    logger.info('Connected to MySQL database')
                    db_conn.close()
                    break
            except:
                pass
            time.sleep(1)
            count = count + 1
            if count > 600:
                start_sucess = False
                logger.info("can not connect to DB")
                break
        logger.info('finish {} seconds waiting for connection'.format(count))
        logger.info('{} --defaults-file={}'.format(self.mysqld, self.mycnf))
        logger.info('mysql is up')
        return start_sucess
    def get_benchmark_cmd(self):
        timestamp = int(time.time())
        filename = self.log_path + '/{}.log'.format(timestamp)
        dirname, _ = os.path.split(os.path.abspath(__file__))
        if self.workload['name'] == 'sysbench':
            cmd = self.workload['cmd'].format(dirname + '/cli/run_sysbench.sh',
                                              self.workload['type'],
                                              self.host,
                                              self.port,
                                              self.user,
                                              150,
                                              800000,
                                              BENCHMARK_WARMING_TIME,
                                              self.threads,
                                              BENCHMARK_RUNNING_TIME,
                                              filename,
                                              self.dbname)

        elif self.workload['name'] == 'tpcc':
            cmd = self.workload['cmd'].format(dirname + '/cli/run_tpcc.sh',
                                              self.host,
                                              self.port,
                                              self.user,
                                              self.threads,
                                              BENCHMARK_WARMING_TIME,
                                              BENCHMARK_RUNNING_TIME,
                                              filename,
                                              self.dbname)


        elif self.workload['name'] == 'oltpbench':
            filename = filename.split('/')[-1].split('.')[0]
            cmd = self.workload['cmd'].format(dirname + '/cli/run_oltpbench.sh',
                                              self.dbname,
                                              self.oltpbench_config_xml,
                                              filename)

        elif self.workload['name'] == 'workload_zoo':
            filename = filename.split('/')[-1].split('.')[0]
            cmd = self.workload['cmd'].format(dirname + '/cli/run_workload_zoo.sh',
                                              self.workload_zoo_app,
                                              self.workload_zoo_config,
                                              filename)

        elif self.workload['name'] == 'job':
            cmd = self.workload['cmd'].format(dirname + '/cli/run_job.sh',
                                              dirname + '/cli/selectedList.txt',
                                              dirname + '/job_query/queries-mysql-new',
                                              filename,
                                              self.sock
                                              )


        logger.info('[DBG]. {}'.format(cmd))
        return cmd, filename

    def _gen_config_file(self, knobs):
        cnf_parser = ConfigParser(self.mycnf)
        konbs_not_in_mycnf = []
        for key in knobs.keys():
            if not key in self.knobs_detail.keys():
                konbs_not_in_mycnf.append(key)
                continue
            cnf_parser.set(key, knobs[key])
        cnf_parser.replace()
        logger.info('generated config file done')
        return konbs_not_in_mycnf

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

    def set_rds_param(self, db_conn, k, v):
        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(k)
        r = db_conn.fetch_results(sql)
        if v == 'ON':
            v = 1
        elif v == 'OFF':
            v = 0
        if r[0]['Value'] == 'ON':
            v0 = 1
        elif r[0]['Value'] == 'OFF':
            v0 = 0
        else:
            try:
                v0 = eval(r[0]['Value'])
            except:
                v0 = r[0]['Value'].strip()
        if v0 == v:
            return True

        IsSession = False
        if str(v).isdigit():
            sql = "SET GLOBAL {}={}".format(k, v)
        else:
            sql = "SET GLOBAL {}='{}'".format(k, v)
        try:
            db_conn.execute(sql)
        except:
            logger.info("Failed: execute {}".format(sql))
            IsSession = True
            if str(v).isdigit():
                sql = "SET {}={}".format(k, v)
            else:
                sql = "SET {}='{}'".format(k, v)
            db_conn.execute(sql)
        while not self._check_apply(db_conn, k, v, v0, IsSession):
            time.sleep(1)
        return True

    def _check_apply(self, db_conn, k, v, v0, IsSession=False):
        if IsSession:
            sql = 'SHOW VARIABLES LIKE "{}";'.format(k)
            r = db_conn.fetch_results(sql)
            if r[0]['Value'] == 'ON':
                vv = 1
            elif r[0]['Value'] == 'OFF':
                vv = 0
            else:
                vv = r[0]['Value'].strip()
            if vv == v0:
                return False
            return True

        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(k)
        r = db_conn.fetch_results(sql)
        if r[0]['Value'] == 'ON':
            vv = 1
        elif r[0]['Value'] == 'OFF':
            vv = 0
        else:
            vv = r[0]['Value'].strip()
        if vv == v0:
            return False
        return True

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
