import h2o
from h2o.estimators.deeplearning import H2ODeepLearningEstimator
import pandas as pd
import numpy as np

if __name__ == '__main__':
    h2o.init(ip="localhost", port=54323)

