import numpy as np
import matplotlib.pylab as plt
import argparse
from io import BytesIO

parser = argparse.ArgumentParser(description='Plots a line plot')
parser.add_argument('--dump_svg', type=bool, default=False,
                   help='If true will not plot to screen, but dump SVG to stdout.')
args = parser.parse_args()

data = {{files_and_labels}}
for filename, label in data:
    data = np.loadtxt(filename)
    x = data[:,0]
    y = data[:,1]
    x, y = zip(*sorted(zip(x, y)))
    plt.plot(x, y, "{{line_type}}", label=label)

ax = plt.gca()
for lines_and_labels in {{lines_and_labels}}:
    next_color = ax._get_lines.get_next_color()
    for x_pos, label in lines_and_labels:
        plt.axvline(x_pos, label=label, color=next_color)

for ranges in {{ranges}}:
    next_color = ax._get_lines.get_next_color()
    for x1, x2 in ranges:
        plt.axvspan(x1, x2, color=next_color)

for a_label, a_x, a_y in {{annotations}}:
    plt.annotate(a_label, xy=(a_x, a_y), xytext=(-20, 20),
        textcoords='offset points', ha='right', va='bottom',
        bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
        arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))

plt.title('{{title}}')
plt.xlabel('{{xlabel}}')
plt.ylabel('{{ylabel}}')

if len(data) > 1:
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
