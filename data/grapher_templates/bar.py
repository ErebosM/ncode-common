import numpy as np
import matplotlib.pyplot as plt
import argparse
from io import BytesIO

parser = argparse.ArgumentParser(description='Plots a bar plot')
parser.add_argument('--dump_svg', type=bool, default=False,
                   help='If true will not plot to screen, but dump SVG to stdout.')
args = parser.parse_args()

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

if args.dump_svg:
    # Save SVG in a fake file object.
    imgdata = BytesIO()
    plt.savefig(imgdata, format="svg")
    svg_img = imgdata.getvalue()
    svg_img = '<svg' + svg_img.split('<svg')[1]
    print svg_img
else:
    plt.show()
