import numpy as np
import matplotlib.pylab as plt
import argparse
from io import BytesIO

parser = argparse.ArgumentParser(description='Plots a CDF')
parser.add_argument('--dump_svg', type=bool, default=False,
                   help='If true will not plot to screen, but dump SVG to stdout.')
args = parser.parse_args()

def PlotCDF(x, label):
    x = np.sort(x)
    y = np.arange(len(x))/float(len(x))
    plt.plot(x, y, label=label)

for filename, label in {{files_and_labels}}:
    data = np.loadtxt(filename)
    PlotCDF(data, label=label)

ax = plt.gca()
for x_pos, label in {{lines_and_labels}}:
    next_color = ax._get_lines.get_next_color()
    plt.axvline(x_pos, label=label, color=next_color)

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
