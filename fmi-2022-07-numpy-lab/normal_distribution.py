from numpy import random
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == '__main__':
    sns.distplot(random.normal(loc=1, scale=10, size=100), hist=True)
    plt.show()