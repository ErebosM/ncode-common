import numpy as np
import matplotlib.pylab as plt

for filename, label in {{files_and_labels}}:
    data = np.loadtxt(filename)
    x = data[:,0]
    y = data[:,1]
    plt.plot(x, y, label=label)

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
plt.show()
