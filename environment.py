import time
import mysql.connector
import math
import subprocess
import os
import psutil
from datetime import datetime
import pyuac
from pyuac import main_requires_admin

knobs_definition = [
    'innodb_adaptive_flushing_lwm',#0-10-70
    #'innodb_adaptive_hash_index',#OFF-ON-ON
    'innodb_adaptive_max_sleep_delay',#0-150000-1000000
    #!!!'innodb_buffer_pool_instances',#1-8-64
    'innodb_buffer_pool_size',#5242880-134217728-2**64-1
    #'innodb_change_buffering',#(none-inserts-deletes-changes-purges-all)-all
    'innodb_io_capacity',#100-200-2**64-1
    #!!!'innodb_log_file_size',#4194304-50331648-(512GB/innodb_log_files_in_group)
    'innodb_max_dirty_pages_pct',#0-75-99.99
    'innodb_max_dirty_pages_pct_lwm',#0-0-99.99
    #!!!'innodb_sync_array_size',#1-1-1024
    'innodb_thread_concurrency',#0-0-1000
    'max_heap_table_size',#16384-16777216-18446744073709550592
    'thread_cache_size',#0-(-1)-16384
    'tmp_table_size',#1024-16777216-18446744073709551615
    'binlog_cache_size',#4096-32768-18446744073709547520
    'binlog_max_flush_queue_time',#0-0-100000
    'binlog_stmt_cache_size',#4096-32768-18446744073709547520
    'eq_range_index_dive_limit',#0-200-4294967295
    'host_cache_size',#0-(-1)-65536
    #'innodb_adaptive_flushing',#OFF-ON-ON
    'innodb_autoextend_increment',#1-64-1000
    #'innodb_buffer_pool_dump_now',#OFF-OFF-ON
    #'innodb_buffer_pool_load_at_startup',#OFF-ON-ON
    #'innodb_buffer_pool_load_now',#OFF-OFF-ON
    'innodb_change_buffer_max_size',#0-25-50
    'innodb_commit_concurrency',#0-0-1000
    'innodb_compression_failure_threshold_pct',#0-5-100
    'innodb_compression_level',#0-6-9
    'innodb_compression_pad_pct_max',#0-50-75
    'innodb_concurrency_tickets',#1-5000-4294967295
    'innodb_flush_log_at_timeout',#1-1-2700
    'innodb_flush_neighbors',#(0-1-2)-1
    'innodb_flushing_avg_loops',#1-30-1000
    #!!!'innodb_ft_cache_size',#1600000-8000000-80000000
    'innodb_ft_result_cache_limit',#1000000-2000000000-2**32-1
    #!!!'innodb_ft_sort_pll_degree',#1-2-32
    'innodb_io_capacity_max',#100-?-2**32-1
    'innodb_lock_wait_timeout',#1-50-1073741824
    'innodb_log_buffer_size',#1048576-16777216-4294967295
    'innodb_lru_scan_depth',#100-1024-2**64-1
    'innodb_max_purge_lag',#0-0-4294967295
    'innodb_max_purge_lag_delay',#0-0-10000000
    'innodb_old_blocks_pct',#5-37-95
    'innodb_old_blocks_time',#0-1000-2**32-1
    'innodb_online_alter_log_max_size',#65536-134217728-2**64-1
    #'innodb_page_size',#(4096-8192-16384-32768-65536)-16384
    'innodb_purge_batch_size',#1-300-5000
    #!!!'innodb_purge_threads',#1-4-32
    #'innodb_random_read_ahead',#OFF-OFF-ON
    'innodb_read_ahead_threshold',#0-56-64
    #!!!'innodb_read_io_threads',#1-4-64
    'innodb_replication_delay',#0-0-4294967295
    'innodb_rollback_segments',#1-128-128
    #!!!'innodb_sort_buffer_size',#65536-1048576-67108864
    'innodb_spin_wait_delay',#0-6-2**64-1
    'innodb_sync_spin_loops',#0-30-4294967295
    'innodb_thread_sleep_delay',#0-10000-1000000
    #'innodb_use_native_aio',#OFF-ON-ON
    #!!!'innodb_write_io_threads',#1-4-64
    'join_buffer_size',#128-262144-4294967168
    'lock_wait_timeout',#1-31536000-31536000
    'max_binlog_cache_size',#4096-18446744073709547520-18446744073709547520
    'max_binlog_size',#4096-1073741824-1073741824
    'max_binlog_stmt_cache_size',#4096-18446744073709547520-18446744073709547520
    'max_delayed_threads',#0-20-16384
    'max_insert_delayed_threads',#0-20-16384
    'max_join_size',#1-18446744073709551615-18446744073709551615
    'max_length_for_sort_data',#4-1024-8388608
    'max_seeks_for_key',#1-4294967295-4294967295
    'max_sort_length',#4-1024-8388608
    'max_sp_recursion_depth',#0-0-255
    'max_write_lock_count',#1-4294967295-4294967295
    #'metadata_locks_cache_size',#1-1024-1048576
    'optimizer_prune_level',#0-1-1
    'optimizer_search_depth',#0-62-62
    'preload_buffer_size',#1024-32768-1073741824
    'query_alloc_block_size',#1024-8192-4294966272
    'query_prealloc_size',#8192-8192-18446744073709550592
    'range_alloc_block_size',#4096-4096-18446744073709550592
    'read_buffer_size',#8192-131072-2147479552
    'read_rnd_buffer_size',#1-262144-2147483647
    'slave_checkpoint_group',#32-512-524280
    'slave_checkpoint_period',#1-300-4294967295
    'slave_parallel_workers',#0-4-1024
    'slave_pending_jobs_size_max',#1024-128M-16EiB
    'sort_buffer_size',#32768-262144-4294967295
    'stored_program_cache',#16-256-524288
    'table_definition_cache',#400-(-1)-524288
    'table_open_cache',#1-2000-524288
    #!!!'table_open_cache_instances',#1-16-64
    #!!!'thread_stack',#131072-262144-18446744073709550592
    'transaction_alloc_block_size',#1024-8192-131072
    'transaction_prealloc_size',#1024-8192-131072
    #'innodb_dedicated_server',#OFF-OFF-ON
    #!!!'innodb_doublewrite_batch_size',#0-0-256
    #!!!'innodb_doublewrite_files',#2-8-256
    #!!!'innodb_doublewrite_pages',#4-4-512
    #!!!'innodb_log_files_in_group',#2-2-100
    'innodb_log_spin_cpu_abs_lwm',#0-80-4294967295
    'innodb_log_spin_cpu_pct_hwm',#0-50-100
    'innodb_log_wait_for_flush_spin_hwm',#0-400-2**64-1
    'max_relay_log_size',#0-0-1073741824
    #!!!'relay_log_space_limit',#0-0-18446744073709551615
    'rpl_read_size',#8192-8192-4294959104
    'stored_program_definition_cache',#256-256-524288
    'tablespace_definition_cache',#256-256-524288
    'temptable_max_ram'#2097152-1073741824-2^64-1
]

class MysqlConnector:
    def __init__(self, host='localhost', user='root', passwd='root', name='sakila'):
        super().__init__()
        self.dbhost = host
        self.dbuser = user
        self.dbpasswd = passwd
        self.dbname = name
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        self.conn = mysql.connector.connect(host=self.dbhost, user=self.dbuser, passwd=self.dbpasswd, db=self.dbname)
        if self.conn:
            self.cursor = self.conn.cursor()

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
        if self.conn:
            self.cursor.execute(sql)

class MySQLEnv:
    def __init__(self, knobs_list, default_knobs):
        super().__init__()
        self.knobs_list = knobs_list
        self.states_list = []
        self.db_con = MysqlConnector()
        self.states = []
        self.score = 0.
        self.steps = 0
        self.terminate = False
        self.last_latency = 0
        self.default_latency = 0
        self.default_knobs = default_knobs

    def db_is_alive(self):
        flag = True
        while flag:
            for proc in psutil.process_iter():
                if proc.name() == "mysqld.exe":
                    flag = False
                    break
            if flag:
                time.sleep(20)
    def apply_knobs(self, knobs):
        self.db_is_alive()
        db_conn = MysqlConnector()
        for i in range(len(knobs_definition)):
            sql = f"SET GLOBAL {knobs_definition[i]} = {knobs[i]}"
            try:
                db_conn.execute(sql)
            except:
                sql = f"SET {knobs_definition[i]} = {knobs[i]}"
                db_conn.execute(sql)
        db_conn.close_db()
        self.db_restart()
    ##

    @main_requires_admin
    def db_restart(self):
        os.system("C:\\Windows\\System32\\net.exe stop MySql80")
        os.system("C:\\Windows\\System32\\net.exe start MySql80")
        #subprocess.call(["\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqladmin.exe\" -u root shutdown -p\"root\""])
        #subprocess.call(["\"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld\" --defaults-file=\"C:\Program Files\MySQL\MySQL Server 8.0\my.ini\" "])

    def get_latency(self):
        self.db_con.connect_db()
        t1 = float(datetime.utcnow().strftime('%S.%f'))
        r = self.db_con.fetch_results("SELECT COUNT(*) FROM actor")
        t2 = float(datetime.utcnow().strftime('%S.%f'))
        self.db_con.close_db()
        return math.fabs(t2-t1)

    def get_internal_metrics(self, internal_metrics):
        db_conn = MysqlConnector()
        sql = 'SELECT NAME, COUNT from information_schema.INNODB_METRICS where status="enabled" ORDER BY NAME'
        res = db_conn.fetch_results(sql)
        res_dict = {}
        for (k, v) in res:
            res_dict[k] = v
        internal_metrics.append(res_dict)
        return internal_metrics

    def get_states(self):
        #internal_metrics = Manager().list()
        internal_metrics = []
        external_metrics = self.get_latency()
        internal_metrics = self.get_internal_metrics(internal_metrics)
        return external_metrics, internal_metrics

    ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
    def step(self, knobs):
        self.steps += 1
        self.apply_knobs(knobs)
        s = self.get_states()
        latency, internal_metrics = s
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

        latency, internal_states = s

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
