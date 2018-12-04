import os

directory = os.fsencode('out')

ctr = 0
current_fh = open('out0', 'w+')
num_files = 0
for f in os.listdir(directory):
  if ctr == 10000:
    current_fh.close()
    current_fh = open(f'out{num_files}', 'w+')
    num_files += 1
  current_fh.write('\n----------.----------\n')
  with open(f, 'r') as fh:
    current_fh.write(fh.read())
  ctr += 1
current_fh.close()
