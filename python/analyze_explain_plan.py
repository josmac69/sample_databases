"""
Experimental code for analyzing
PostgreSQL EXPLAIN / EXPLAIN ANALYZE output
and visualizing the query plan.
"""
import re
import matplotlib.pyplot as plt
import textwrap
import argparse

def format_size(size_str):
    match = re.match(r"(\d+(\.\d+)?)\s?([\w]+)", size_str)
    if match:
        num, unit = match.groups()
        formatted_num = "{:,}".format(int(num))
        return f"{formatted_num} {unit}"
    return size_str

parser = argparse.ArgumentParser(description='Process PostgreSQL EXPLAIN ANALYZE output')
parser.add_argument('--file', type=str, required=True, help='Path and name of the file containing EXPLAIN ANALYZE output.')

args = parser.parse_args()
filename = args.file

try:
    with open(filename, 'r') as file:
        explain_lines = file.readlines()
except IOError:
    print(f"Cannot read file: {filename}")
    sys.exit(1)

# Parse the EXPLAIN ANALYZE output
# operation_pattern = r"\s*(->\s*)?([\w\s]+)\s+\(.*?\)\s+\(actual time=(\d+\.\d+)..(\d+\.\d+) rows=(\d+)"
operation_pattern = r"\s*(?:->\s*)?([^\()]+)\s+\(.*?\)\s+\(actual time=(\d+\.\d+)..(\d+\.\d+) rows=(\d+) loops=(\d+)"
buffers_pattern = r"Buffers: (.+)"
heap_blocks_pattern = r"Heap Blocks: exact=([\d+])"
planning_time_pattern = f"Planning Time: ([\d.]+) ms"
execution_time_pattern = r"Execution Time: ([\d.]+) ms"
sort_method_pattern = f"Sort Method: (\w+(?: \w+)?) .*(Memory|Disk): (\d+[\w]+)"

explain_plan_graph_file = 'explain_plan.png'

parts = []
start_times = []
end_times = []
rows = []
loops = []
buffers = []
heap_blocks = []
sort_method_info = []

current_sort_method=""

for line in reversed(explain_lines):
    # cleaned_line = part.lstrip("'").strip().lstrip("->").strip()
    cleaned_line = line.lstrip("('").lstrip('("').strip()
    # print(f"cleaned_line: {cleaned_line}")

    operation_match = re.match(operation_pattern, cleaned_line)
    buffer_match = re.match(buffers_pattern, cleaned_line)
    heap_blocks_match = re.match(heap_blocks_pattern, cleaned_line)
    planning_time_match = re.match(planning_time_pattern, cleaned_line)
    execution_time_match = re.match(execution_time_pattern, cleaned_line)
    sort_method_match = re.match(sort_method_pattern, cleaned_line)

    # print(f"planning_time_match: {planning_time_match}")
    if planning_time_match:
        planning_time = float(planning_time_match.group(1))

    # print(f"sort_method_match: {sort_method_match}")
    if sort_method_match:
        current_sort_method = f"\n{sort_method_match.group(1)} {sort_method_match.group(2)}\n{format_size(sort_method_match.group(3))}"

    # print(f"operation_match: {operation_match}")
    if operation_match:
        part, start, end, row_count, loop_count = operation_match.groups()
        parts.append(part)
        start_times.append(float(start))
        end_times.append(float(end))
        rows.append(int(row_count))
        loops.append(int(loop_count))
        sort_method_info.append(current_sort_method)
        current_sort_method=""

    # print(f"execution_time_match: {execution_time_match}")
    if execution_time_match:
        execution_time = float(execution_time_match.group(1))
        parts.append("Total Execution Time")
        start_times.append(0)
        end_times.append(execution_time)
        rows.append(0)
        loops.append(1)
        sort_method_info.append("")

wrapped_parts=['\n'.join(textwrap.wrap(p, width=20)) for p in parts]
colors = plt.cm.tab10.colors

print(f"planning_time: {planning_time}")
print(f"execution_time: {execution_time}")
print(f"parts: {parts}")
print(f"sort_method_info: {sort_method_info}")

#print(f"parts: {parts}")
# Create the plot
fig, ax = plt.subplots(figsize=(10,6))
fig.subplots_adjust(left=0.3, bottom=0.3)

legend_handles = []
legend_labels = []

# Plot each part on the timeline
for i, part in enumerate(parts):
    if i==0 and part=='Total Execution Time':
        continue

    color = colors[i % len(colors)]
    mid_point = (start_times[i] + end_times[i]) / 2
    # ax.plot([cumulative_start_times[i], cumulative_end_times[i]], [i, i], marker='|', markersize=10, label=part)
    line, = ax.plot([start_times[i], end_times[i]], [i, i], marker='|', markersize=10, label=part, color=color)

    ax.axvline(x=start_times[i], color=color, linestyle=':', alpha=0.7, ymax=(i + 0.4)/len(parts))
    ax.axvline(x=end_times[i], color=color, linestyle=':', alpha=0.7, ymax=(i + 0.4)/len(parts))

    if sort_method_info[i]:
        ax.annotate(sort_method_info[i],
                    xy=(mid_point, i),
                    xytext=(0,0),
                    textcoords='offset points',
                    ha='center',
                    va='top',
                    # arrowprops=dict(arrowstyle="->", color='grey'),
                    fontsize=6)
    # Add a bubble for the row count
    # ax.scatter(cumulative_end_times[i], i, s=rows[i]/1000, alpha=0.5)  # scaling down row count for visibility
    bubble_size = 1000 * (rows[i] / max(rows))
    bubble = ax.scatter(mid_point, i + 0.3, s=bubble_size, alpha=0.5, color=color)  # scaling down row count for visibility

    if i < len(parts)-1:
        bubble_text=f"rows: {rows[i]:,}\nloops: {loops[i]:,}"
    else:
        bubble_text=f"Total rows: {rows[len(parts)-2]:,}"

    plt.text(mid_point, i + 0.3, bubble_text, color='black', ha='left', va='center', fontsize=6)

    ax.axhline(y=i, color='lightgrey', linestyle=':', alpha=0.7)

    legend_handles.append(line)
    legend_labels.append(parts[i])

legend_handles.reverse()
legend_labels.reverse()

ax.annotate(f'Planning Time:\n{planning_time:.2f} ms',
            xy=(planning_time, len(parts)-1),
            xytext=(planning_time + 20, len(parts)-0.8),
            arrowprops=dict(facecolor='red', arrowstyle='->'),
            fontsize=6,
            ha='left')

ax.annotate(f'Total Execution Time:\n{execution_time:.2f} ms',
            xy=(execution_time, len(parts)-1),
            xytext=(execution_time + 10, len(parts)-0.8),
            arrowprops=dict(facecolor='red', arrowstyle='->'),
            fontsize=6,
            ha='right')

# Formatting the graph
ax.set_xlabel('Run Time (ms)', fontsize=6)
ax.set_ylabel('Query Parts', fontsize=6)
ax.set_yticks(range(len(parts)))
ax.set_yticklabels(wrapped_parts, fontsize=6)
#ax.set_xticklabels(ax.get_xticks(), fontsize=6)
# ax.set_xlim(0, cumulative_end_times[-1] + 100)  # adding some space for clarity
ax.set_xlim(0, max(end_times) + 100)  # adding some space for clarity
ax.set_title('PostgreSQL EXPLAIN ANALYZE output visualization', fontsize=10)
ax.legend(legend_handles, legend_labels, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=1, fontsize=6)

plt.savefig(explain_plan_graph_file, dpi=300)
plt.close()
