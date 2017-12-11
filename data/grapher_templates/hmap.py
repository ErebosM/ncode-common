import numpy as np
import matplotlib.pylab as plt
import argparse
from io import BytesIO
import matplotlib.colors as colors

parser = argparse.ArgumentParser(description='Plots a CDF')
parser.add_argument('--dump_svg', type=bool, default=False,
                   help='If true will not plot to screen, but dump SVG to stdout.')
args = parser.parse_args()

all_data = []
for filename, label in {{files_and_labels}}:
    data = np.loadtxt(filename)
    all_data.append(data)
all_data = np.vstack(all_data)

ax = plt.gca()
for x_pos, label in {{lines_and_labels}}:
    next_color = ax._get_lines.get_next_color()
    plt.axvline(x_pos, label=label, color=next_color)

fig = plt.gcf()

if {{log_scale}}:
    im = ax.imshow(all_data, norm=colors.SymLogNorm(1.0, vmin=np.min(all_data), vmax=np.max(all_data)),
                   cmap='hot', interpolation='nearest')
else:
    im = ax.imshow(all_data, cmap='hot', interpolation='nearest')
fig.colorbar(im)

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
