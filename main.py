import tensorflow as tf
from tensorflow import keras
import numpy as np
import math

class ParameterNoise(keras.layers.Layer):
    def __init__(self, units):
        super(ParameterNoise, self).__init__()
        self.units = units
        self.sigma_init_value = 0.05

    def build(self, input_shape):
        w_init = tf.random_uniform_initializer(-math.sqrt(3 / self.units), math.sqrt(3 / self.units))
        self.w = tf.Variable(initial_value=w_init(shape=(input_shape[-1], self.units)), trainable=True)
        b_init = tf.random_uniform_initializer(-math.sqrt(3 / self.units), math.sqrt(3 / self.units))
        self.b = tf.Variable(initial_value=b_init(shape=(self.units,)), trainable=True)

        sigma_init = tf.keras.initializers.Constant(value=self.sigma_init_value)
        self.sigma_w = tf.Variable(initial_value=sigma_init(shape=(input_shape[-1], self.units)), trainable=True)
        self.sigma_b = tf.Variable(initial_value=sigma_init(shape=(self.units,)), trainable=True)
        self.epsilon_w = tf.Variable(initial_value=tf.zeros((input_shape[-1], self.units)), trainable=False)
        self.epsilon_b = tf.Variable(initial_value=tf.zeros((self.units,)), trainable=False)

    def call(self, inputs):
        return tf.matmul(inputs, self.w+self.sigma_w*self.epsilon_w) + (self.b+self.sigma_b*self.epsilon_b)

    def sample_noise(self):
        self.epsilon_w = tf.random.uniform(shape=(self.units[-1], self.units))
        self.epsilon_b = tf.random.uniform(shape=(self.units,))

class OUActionNoise:
    def __init__(self, mean, std_deviation, theta=0.15, dt=1e-2, x_initial=None):
        self.theta = theta
        self.mean = mean
        self.std_dev = std_deviation
        self.dt = dt
        self.x_initial = x_initial
        self.reset()

    def __call__(self):
        # Formula taken from https://www.wikipedia.org/wiki/Ornstein-Uhlenbeck_process.
        x = (
            self.x_prev
            + self.theta * (self.mean - self.x_prev) * self.dt
            + self.std_dev * np.sqrt(self.dt) * np.random.normal(size=self.mean.shape)
        )
        # Store x into x_prev
        # Makes next noise dependent on current one
        self.x_prev = x
        return x

    def reset(self):
        if self.x_initial is not None:
            self.x_prev = self.x_initial
        else:
            self.x_prev = np.zeros_like(self.mean)

def get_model_actor(states, actions):
    last_init = tf.random_uniform_initializer(minval=-0.003, maxval=0.003)
    inputs = keras.Input((states,))
    x = keras.layers.Dense(units=128)(inputs)
    x = keras.layers.LeakyReLU(alpha=0.2)(x)
    x = keras.layers.BatchNormalization()(x)
    x = keras.layers.Dense(units=128)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)

    x = keras.layers.Dense(units=64)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    x = keras.layers.Dense(units=actions, activation="sigmoid", kernel_initializer=last_init)(x)
    outputs = ParameterNoise(units=actions)(x)

    return keras.Model(inputs, outputs, name="actor")

def get_model_critic(states, actions):
    state_input = keras.layers.Input((states,))
    x_s = keras.layers.Dense(units=128)(state_input)

    action_input = keras.layers.Input((actions,))
    x_a = keras.layers.Dense(units=128)(action_input)

    concat = keras.layers.Concatenate()([x_s, x_a])

    x = keras.layers.Dense(units=256)(concat)
    x = keras.layers.LeakyReLU(alpha=0.2)(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    x = keras.layers.Dense(units=64)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    outputs = keras.layers.Dense(units=1, activation="tanh")(x)

    return keras.Model([state_input, action_input], outputs, name="critic")


#m1 = get_model_actor(64,64)
#m2 = get_model_critic(64,64)
#m1.summary()
#m2.summary()
