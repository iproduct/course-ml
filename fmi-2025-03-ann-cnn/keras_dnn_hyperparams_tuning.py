import keras_tuner
import keras
import numpy as np
from keras.api.datasets import mnist

num_classes = 10

# input image dimensions
img_rows, img_cols = 28, 28
input_shape = (img_rows, img_cols, 1)

def build_model(hp):
  model = keras.Sequential()
  model.add(keras.layers.Flatten(input_shape=(28, 28)),)
  model.add(keras.layers.Dense(
      hp.Choice('units', [8, 16, 32, 64]),
      activation='relu'))
  model.add(keras.layers.Dense(num_classes, activation='softmax'))
  model.compile(optimizer=keras.optimizers.Adam(),  loss="categorical_crossentropy", metrics=["accuracy"])
  print(model.summary())
  return model

tuner = keras_tuner.RandomSearch(
    build_model,
    objective='val_accuracy',
    max_trials=5,
    executions_per_trial=2,
    overwrite=True,
    directory="tuner_data",
    project_name="mnist_hyperparams_tuning",
)


if __name__ == '__main__':
    # the data, shuffled and split between train and test sets
    (x_train, y_train), (x_test, y_test) = mnist.load_data()

    # x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
    # x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)

    x_train = x_train.astype('float32')
    x_test = x_test.astype('float32')
    x_train /= 255
    x_test /= 255
    print('x_train shape:', x_train.shape)
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')

    x_train = np.expand_dims(x_train, -1)
    x_test = np.expand_dims(x_test, -1)

    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    # build the model
    # hp = keras_tuner.HyperParameters()
    # model = build_model(hp)

    tuner.search_space_summary()

    tuner.search(x_train, y_train, epochs=5, validation_data=(x_test, y_test))
    best_model = tuner.get_best_models()[0]

    print('BEST MODEL SUMMARY:\n', best_model.summary())

    tuner.results_summary()