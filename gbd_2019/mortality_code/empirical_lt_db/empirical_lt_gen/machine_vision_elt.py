"""
Machine vision code for ELT vetting

Pre-requisite: run R code to generate test and
        train directories of .jpg plots
"""

from sklearn.metrics import confusion_matrix
import numpy as np
import pandas as pd
import glob  # For including images
import cv2  # opencv-python
import os
import progressbar
from tensorflow import keras
import matplotlib.pyplot as plt
import logging
import datetime
import psutil


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def load_plot_set(dir, type):
    """
    function to load set of .jpgs
    type = 'test' or 'train'
    """
    if (type != 'test' and type != 'train' and type != 'predict' and type != 'smooth'):
        raise ValueError('type argument should be test, train, predict, or smooth')
    data_img = []
    label = []
    index = []
    for dir_path in glob.glob("{}/{}/*".format(dir, type)):
        img_label = dir_path.split("/")[-1]
        num_files = len(glob.glob(os.path.join(dir_path, "*.jpg")))
        logging.info('Loading {} images from {}'.format(num_files, dir_path))
        bar = progressbar.ProgressBar(maxval=num_files,
                                      widgets=[progressbar.Percentage()])
        bar.start()
        i = 0
        for img_path in glob.glob(os.path.join(dir_path, "*.jpg")):
            img = cv2.imread(img_path)
            img = cv2.resize(img, (384, 384))
            data_img.append(img)
            label.append(img_label)
            bar.update(i+1)
            index.append(img_path)
            i += 1
    data_img = np.array(data_img)
    label = np.array(label)
    return label, data_img, index


def make_outlier_ids(train_label, test_label):
    """
    make identifiers (outlier / non_outliers) into
    binary (outlier = 1, non_outlier = 0) and
    """
    logging.info('Saving binary ids for outlier status')
    label_to_id = {v: k for k, v in enumerate(np.unique(train_label))}
    train_label_id = np.array([label_to_id[i] for i in train_label])
    test_label_id = np.array([label_to_id[i] for i in test_label])
    return train_label_id, test_label_id


def prep_image(img):
    """
    reduce image size
    """
    logging.info('Reducing image size')
    img = img / 255.0
    return img


def prep_model():
    """
    define keras model object
    """
    logging.info('Defining keras model object')
    model = keras.Sequential()
    model.add(keras.layers.Conv2D(16, (3, 3),
                                  input_shape=(384, 384, 3),
                                  padding="same",
                                  activation="relu"))
    model.add(keras.layers.MaxPooling2D(pool_size=(2, 2)))

    model.add(keras.layers.Conv2D(32, (3, 3),
                                  padding="same",
                                  activation="relu"))
    model.add(keras.layers.MaxPooling2D(pool_size=(2, 2)))

    model.add(keras.layers.Conv2D(32, (3, 3),
                                  padding="same",
                                  activation="relu"))
    model.add(keras.layers.MaxPooling2D(pool_size=(2, 2)))

    model.add(keras.layers.Conv2D(64, (3, 3),
                                  padding="same",
                                  activation="relu"))
    model.add(keras.layers.MaxPooling2D(pool_size=(2, 2)))

    model.add(keras.layers.Flatten())
    model.add(keras.layers.Dense(256, activation="relu"))
    model.add(keras.layers.Dense(75, activation="softmax"))

    return model


def run_model(model, train_data_img, train_label_id,
              test_data_img, test_label_id, num_epochs,
              dir):
    """
    run model
    """
    logging.info('Running model')
    model.compile(loss="sparse_categorical_crossentropy",
                  optimizer=keras.optimizers.Adamax(),
                  metrics=['accuracy'])
    tensorboard = keras.callbacks.TensorBoard(log_dir="./Graph",
                                              histogram_freq=0,
                                              write_graph=True,
                                              write_images=True)
    history = model.fit(train_data_img,
                        train_label_id,
                        batch_size=256,
                        epochs=num_epochs,
                        callbacks=[tensorboard],
                        validation_data=(test_data_img, test_label_id))
    now = datetime.datetime.now()
    stamp = now.strftime("%Y-%m-%d-%H-%M")
    model_path = '{}/model_{}.h5'.format(dir, stamp)
    model.save(model_path)
    return model, history, model_path


def collect_predictions(model, test_data_img):
    """
    collect prediction of models,
    output a list predicted by model
    """
    logging.info('Making predictions')
    predictions = model.predict(test_data_img)
    pred = []
    for i in range(len(predictions)):
        # pred.append(np.argmax(predictions[i]))
        # collect probability a plot is an outlier
        pred.append(predictions[i][1])
    return pred


def accuracy_and_loss(model, test_data_img, test_label_id, pred, history, dir):
    """
    evaluate accuracy and loss of model
    create confusion matrix to check true pos, false pos, true neg, false neg
    plot accuracy over epoch
    """
    logging.info('Calculating accuracy and loss')
    loss, accuracy = model.evaluate(test_data_img, test_label_id)
    logging.info("Loss: {}".format(loss))
    logging.info("Accuracy: {}".format(accuracy))
    print(history.history)
    # plot accuracy
    plt.plot(history.history['acc'])
    plt.plot(history.history['val_acc'])
    plt.title('model accuracy')
    plt.ylabel('accuracy')
    plt.xlabel('epoch')
    plt.legend(['train', 'test'], loc='upper left')
    plt.savefig('{}/accuracy_elt.jpg'.format(dir))
    plt.clf()
    plt.cla()
    plt.close()
    # plot loss
    plt.plot(history.history['loss'])
    plt.plot(history.history['val_loss'])
    plt.title('model loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'test'], loc='upper left')
    plt.savefig('{}/loss_elt.jpg'.format(dir))


def save_predictions(index, label_id, pred, dir, type, subdir):
    """
    create a csv that contains filepath of all files in test,
    the value in the database (outlier/not_outlier),
    and the predicted value
    """
    logging.info('Saving predictions')
    csv_complete = pd.DataFrame(
        {'file_path': index,
         'actual': label_id,
         'predicted': pred})
    if subdir == 'none':
        out_path = "{}/elt_predictions_{}.csv".format(dir, type)
    else:
        out_path = "{}/elt_predictions_{}_{}.csv".format(dir, type, subdir)
    csv_complete.to_csv(out_path, index=False)


def load_plot_set_one_dir(dir_path):
    """
    function to load set of .jpgs
    type = 'test' or 'train'
    """
    data_img = []
    label = []
    index = []
    img_label = dir_path.split("/")[-1]
    num_files = len(glob.glob(os.path.join(dir_path, "*.jpg")))
    logging.info('Loading {} images from {}'.format(num_files, dir_path))
    bar = progressbar.ProgressBar(maxval=num_files,
                                  widgets=[progressbar.Percentage()])
    bar.start()
    i = 0
    for img_path in glob.glob(os.path.join(dir_path, "*.jpg")):
        img = cv2.imread(img_path)
        img = cv2.resize(img, (384, 384))
        data_img.append(img)
        label.append(img_label)
        bar.update(i+1)
        index.append(img_path)
        i += 1
    data_img = np.array(data_img)
    label = np.array(label)
    return label, data_img, index


def predict_many(model_path, dir, type):
    """
    Function to predict many small directories from an exisitng model
    Used to handle high memory needs when predicting many plots
    Type: likely "predict" or "smooth"
    """
    model = keras.models.load_model(model_path)
    for dir_path in glob.glob("{}/{}/*".format(dir, type)):
        logging.info('Working on {}'.format(dir_path))
        print(psutil.virtual_memory())
        label, img, data_index = load_plot_set_one_dir(dir_path)
        img = prep_image(img)
        pred = collect_predictions(model, img)
        subdir = dir_path.split("/")[-1]
        save_predictions(data_index, label, pred, dir, type, subdir)
        del(label, img, data_index, pred)
