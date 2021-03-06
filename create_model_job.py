"""create_model.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1MZnZyDJE0Hma0vbUyQiqRN5kR2x3eH6K
"""


from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from imdb import IMDb
import os, requests

import matplotlib.pyplot as plt
import seaborn as sns

import keras
from keras.models import Sequential
from keras.layers import Dense, Conv2D , MaxPool2D , Flatten , Dropout 
from keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.optimizers import Adam

import tensorflow as tf

import cv2

import numpy as np

app_name = 'final_project'

data_lake_path = "hdfs://namenode:8020/data_lake"

conf = SparkConf()

hdfs_host = 'hdfs://namenode:8020'

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession\
  .builder\
  .appName(app_name)\
  .config(conf=conf)\
  .getOrCreate()


movies = spark.read\
              .option("header", "true")\
              .csv(f'{data_lake_path}/FactMovies.csv')

genres = spark.read\
              .option("header", "true")\
              .csv(f'{data_lake_path}/DimGenre.csv')            


movies_ids = movies\
        .limit(100)\
        .select('id', 'imdb_id', 'title')

movies_ids.show()


movies_data = movies_ids\
                  .join(genres, f.col('id') == f.col('movie_id'), 'left')\
                  .select('imdb_id', 'title', 'genre')\
                  .where(f.col('genre').isNotNull())

movies_data.show()

genres_list = [i.genre for i in movies_data.select('genre').distinct().collect()]

genres_list

datasets = ['training', 'test']

for dataset in datasets:
  path = os.path.join(os.getcwd(), dataset)
  if not os.path.exists(path):
    os.mkdir(path)

def create_dataset_dir(dataset, genre):
  path = os.path.join(os.getcwd(), f"{dataset}/{genre}")
  print(f"path is {path}")
  if not os.path.exists(path):
    os.mkdir(path)


def download_poster(imdb_id, genre, dataset):
  ia = IMDb()

  movie = ia.get_movie(imdb_id)

  res = requests.get(movie['cover url'])
  path = f'{dataset}/{genre}/{imdb_id}.jpg'
  path = os.path.join(os.getcwd(), path)
  file = open(path, 'wb')
  file.write(res.content)
  file.close()

def download_training_posters(genre, training_imdb_ids):
  for i in training_imdb_ids:
    download_poster(i, genre, 'training')

def download_test_posters(genre, test_imdb_ids):
  for i in test_imdb_ids:
    download_poster(i, genre, 'test')


for genre in genres_list:
  genre_movies = movies_data\
                        .select('imdb_id')\
                        .where(f"genre = '{genre}'")
  genre_movies_as_list = [e[0] for e in genre_movies.select('imdb_id').rdd.collect()]

  print(f"{genre}, {genre_movies_as_list}")

  num_genres = len(genre_movies_as_list)

  create_dataset_dir("training", genre)
  create_dataset_dir("test", genre)

  test_data = genre_movies_as_list[0:((int)(num_genres/5))] # 20% test data
  training_data = genre_movies_as_list[((int)(num_genres/5)):] # 80% training data

  download_training_posters(genre, training_data)
  download_test_posters(genre, test_data)

# START POSTER CLASSIFICATION WITH KERAS

labels = genres_list
img_size = 224
def get_data(data_dir):
    data = [] 
    for label in labels: 
        path = os.path.join(data_dir, label)
        class_num = labels.index(label)
        for img in os.listdir(path):
            try:
                img_arr = cv2.imread(os.path.join(path, img))[...,::-1] #convert BGR to RGB format
                resized_arr = cv2.resize(img_arr, (img_size, img_size)) # Reshaping images to preferred size
                data.append([resized_arr, class_num])
            except Exception as e:
                print(e)
    return np.array(data)

train = get_data('training')
val = get_data('test')

l = []
for i in train:
  for index in range(len(labels)):
    if(i[1] == index):
      l.append(labels[index])

sns.set(rc={'figure.figsize':(18,9)})
sns.set_style('darkgrid')
sns.countplot(l)

plt.figure(figsize = (5,5))
plt.imshow(train[1][0])
plt.title(labels[train[0][1]])

plt.figure(figsize = (5,5))
plt.imshow(train[-1][0])
plt.title(labels[train[-1][1]])

x_train = []
y_train = []
x_val = []
y_val = []

for feature, label in train:
  x_train.append(feature)
  y_train.append(label)

for feature, label in val:
  x_val.append(feature)
  y_val.append(label)

# Normalize the data
x_train = np.array(x_train) / 255
x_val = np.array(x_val) / 255

x_train.reshape(-1, img_size, img_size, 1)
y_train = np.array(y_train)

x_val.reshape(-1, img_size, img_size, 1)
y_val = np.array(y_val)

datagen = ImageDataGenerator(
        featurewise_center=False,  # set input mean to 0 over the dataset
        samplewise_center=False,  # set each sample mean to 0
        featurewise_std_normalization=False,  # divide inputs by std of the dataset
        samplewise_std_normalization=False,  # divide each input by its std
        zca_whitening=False,  # apply ZCA whitening
        rotation_range = 30,  # randomly rotate images in the range (degrees, 0 to 180)
        zoom_range = 0.2, # Randomly zoom image 
        width_shift_range=0.1,  # randomly shift images horizontally (fraction of total width)
        height_shift_range=0.1,  # randomly shift images vertically (fraction of total height)
        horizontal_flip = True,  # randomly flip images
        vertical_flip=False)  # randomly flip images


datagen.fit(x_train)

model = Sequential()
model.add(Conv2D(32,3,padding="same", activation="relu", input_shape=(224,224,3)))
model.add(MaxPool2D())

model.add(Conv2D(32, 3, padding="same", activation="relu"))
model.add(MaxPool2D())

model.add(Conv2D(64, 3, padding="same", activation="relu"))
model.add(MaxPool2D())
model.add(Dropout(0.4))

model.add(Flatten())
model.add(Dense(128,activation="relu"))
model.add(Dense(len(labels), activation="softmax"))

model.summary()

opt = Adam(lr=0.000001)
model.compile(optimizer = opt , loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True) , metrics = ['accuracy'])

history = model.fit(x_train,y_train,epochs = 100 , validation_data = (x_val, y_val))