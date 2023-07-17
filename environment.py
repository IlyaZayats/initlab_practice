import time
import mysql.connector
import math

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

    ###
    def apply_knobs(self, knobs):
        self.db_con.connect_db()
        res = False
        return res

    ###
    def get_states(self):
        self.db_con.connect_db()

    ###
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
            print("Waiting 10 seconds. apply_knobs")
            time.sleep(20)
            flag = self.apply_knobs(self.default_knobs)

        s = self.get_states()
        while not s:
            print("Waiting 10 seconds. get_states")
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
