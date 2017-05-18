import numpy as np
import matplotlib.pyplot as plt

def AutoLabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

def PlotBar(data, categories):
    data_size = len(categories)
    sizes = [len(i[0]) for i in data]
    assert(sizes.count(sizes[0]) == len(sizes))

    # Will leave 1 bar distance between the groups. 
    bar_width = 1.0 / (data_size + 1)

    indices = np.arange(data_size)
    for i, rest in enumerate(data):
        values, label = rest
        rects = plt.bar(indices + i * bar_width, values, bar_width, label=label)
        AutoLabel(rects)

    ax = plt.gca()
    ax.set_xticks(indices + ((1.0 - bar_width) / 2.0))
    ax.set_xticklabels(categories)

data = []
for filename, label in {{files_and_labels}}:
    data.append((np.loadtxt(filename), label))

PlotBar(data, {{categories}})

plt.title('{{title}}')
plt.xlabel('{{xlabel}}')
plt.ylabel('{{ylabel}}')
plt.legend()
plt.show()
