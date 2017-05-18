import numpy as np
import matplotlib.pylab as plt

for filename, label in {{files_and_labels}}:
    data = np.loadtxt(filename)
    x = data[:,0]
    y = data[:,1]
    plt.plot(x, y, label=label)

plt.title('{{title}}')
plt.xlabel('{{xlabel}}')
plt.ylabel('{{ylabel}}')
plt.legend()
plt.show()
