import numpy as np
import matplotlib.pylab as plt
import argparse
from io import BytesIO

parser = argparse.ArgumentParser(description='Plots a CDF')
parser.add_argument('--dump_svg', type=bool, default=False,
                   help='If true will not plot to screen, but dump SVG to stdout.')
args = parser.parse_args()

xs = sorted(np.loadtxt({{stacked_plot_xs_filename}}))
ys = []
labels = []
for filename, label in {{files_and_labels}}:
    data = np.loadtxt(filename)
    x = data[:,0]
    y = data[:,1]
    x, y = zip(*sorted(zip(x, y)))

    # Need to interpolate along xs
    y = np.interp(xs, x, y)
    ys.append(y)

    labels.append(label)
plt.stackplot(xs, ys, labels=labels)

ax = plt.gca()
for lines_and_labels in {{lines_and_labels}}:
    next_color = ax._get_lines.get_next_color()
    for x_pos, label in lines_and_labels:
        plt.axvline(x_pos, label=label, color=next_color)

for ranges in {{ranges}}:
    next_color = ax._get_lines.get_next_color()
    for x1, x2 in ranges:
        plt.axvspan(x1, x2, color=next_color)

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
