import tensorflow as tf
from tensorflow import keras
import numpy as np
import math



states_list =

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


states_amount = 3
knobs_amount = 100
class Buffer:
    def __init__(self, capacity=100000, batch_size=16):
        self.capacity = capacity
        self.batch_size = batch_size
        self.counter = 0
        self.state_buffer = np.zeros((self.capacity, states_amount))
        self.action_buffer = np.zeros((self.capacity, knobs_amount))
        self.reward_buffer = np.zeros((self.capacity, 1))
        self.next_state_buffer = np.zeros((self.capacity, states_amount))

    def record(self, observation):
        index = self.counter % self.capacity
        self.state_buffer[index] = observation[0]
        self.action_buffer[index] = observation[1]
        self.reward_buffer[index] = observation[2]
        self.next_state_buffer[index] = observation[3]
        self.counter += 1

    @tf.function
    def update(self, state_batch, action_batch, reward_batch, next_state_batch,):
        with tf.GradientTape() as tape:
            target_actions = target_actor(next_state_batch, training=True)
            y = reward_batch + gamma * target_critic([next_state_batch, target_actions], training=True)
            critic_value = critic_model([state_batch, action_batch], training=True)
            critic_loss = tf.math.reduce_mean(tf.math.square(y - critic_value))

        critic_grad = tape.gradient(critic_loss, critic_model.trainable_variables)
        critic_optimizer.apply_gradients(zip(critic_grad, critic_model.trainable_variables))

        with tf.GradientTape() as tape:
            actions = actor_model(state_batch, training=True)
            critic_value = critic_model([state_batch, actions], training=True)
            # Used `-value` as we want to maximize the value given
            # by the critic for our actions
            actor_loss = -tf.math.reduce_mean(critic_value)

        actor_grad = tape.gradient(actor_loss, actor_model.trainable_variables)
        actor_optimizer.apply_gradients(zip(actor_grad, actor_model.trainable_variables))

    # We compute the loss and update parameters
    def learn(self):
        # Get sampling range
        record_range = min(self.counter, self.capacity)
        # Randomly sample indices
        batch_indices = np.random.choice(record_range, self.batch_size)

        # Convert to tensors
        state_batch = tf.convert_to_tensor(self.state_buffer[batch_indices])
        action_batch = tf.convert_to_tensor(self.action_buffer[batch_indices])
        reward_batch = tf.convert_to_tensor(self.reward_buffer[batch_indices])
        reward_batch = tf.cast(reward_batch, dtype=tf.float32)
        next_state_batch = tf.convert_to_tensor(self.next_state_buffer[batch_indices])

        self.update(state_batch, action_batch, reward_batch, next_state_batch)


@tf.function
def update_target(target_weights, weights, tau):
    for (a, b) in zip(target_weights, weights):
        a.assign(b * tau + a * (1 - tau))
def get_model_actor(n_states, n_actions):
    last_init = tf.random_uniform_initializer(minval=-0.003, maxval=0.003)
    inputs = keras.Input((n_states,))
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
    x = keras.layers.Dense(units=n_actions, activation="sigmoid", kernel_initializer=last_init)(x)
    outputs = ParameterNoise(units=n_actions)(x)

    return keras.Model(inputs, outputs, name="actor")

def get_model_critic(n_states, n_actions):
    state_input = keras.layers.Input((n_states,))
    x_s = keras.layers.Dense(units=128)(state_input)

    action_input = keras.layers.Input((n_actions,))
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

def policy(state, noise_object):
    sampled_actions = tf.squeeze(actor_model(state))
    # Adding noise to action
    for i in range(len(sampled_actions)):
        noise = noise_object()
        sampled_actions[i] = sampled_actions[i].numpy() + noise

    # We make sure action is within bounds
    legal_actions = np.clip(sampled_actions, lower_bound, upper_bound)

    return [np.squeeze(legal_action)]


std_dev = 0.2
ou_noise = OUActionNoise(mean=np.zeros(1), std_deviation=float(std_dev) * np.ones(1))

actor_model = get_model_actor(states_amount, knobs_amount)
critic_model = get_model_critic(states_amount, knobs_amount)

target_actor = get_model_actor(states_amount, knobs_amount)
target_critic = get_model_critic(states_amount, knobs_amount)

# Making the weights equal initially
target_actor.set_weights(actor_model.get_weights())
target_critic.set_weights(critic_model.get_weights())

# Learning rate for actor-critic models
critic_lr = 0.002
actor_lr = 0.001

critic_optimizer = tf.keras.optimizers.Adam(critic_lr)
actor_optimizer = tf.keras.optimizers.Adam(actor_lr)

total_episodes = 100
# Discount factor for future rewards
gamma = 0.99
# Used to update target networks
tau = 0.005

buffer = Buffer(50000, 16)

ep_reward_list = []
# To store average reward history of last few episodes
avg_reward_list = []

# Takes about 4 min to train
for ep in range(total_episodes):

    prev_state = env.reset()
    episodic_reward = 0

    while True:
        # Uncomment this to see the Actor in action
        # But not in a python notebook.
        # env.render()

        tf_prev_state = tf.expand_dims(tf.convert_to_tensor(prev_state), 0)

        actions = policy(tf_prev_state, ou_noise)
        # Recieve state and reward from environment.
        state, reward, done, info = env.step(actions)

        buffer.record((prev_state, actions, reward, state))
        episodic_reward += reward

        buffer.learn()
        update_target(target_actor.variables, actor_model.variables, tau)
        update_target(target_critic.variables, critic_model.variables, tau)

        # End this episode when `done` is True
        if done:
            break

        prev_state = state

    ep_reward_list.append(episodic_reward)

    # Mean of last 40 episodes
    avg_reward = np.mean(ep_reward_list[-40:])
    print("Episode * {} * Avg Reward is ==> {}".format(ep, avg_reward))
    avg_reward_list.append(avg_reward)

#m1 = get_model_actor(64,64)
#m2 = get_model_critic(64,64)
#m1.summary()
#m2.summary()
