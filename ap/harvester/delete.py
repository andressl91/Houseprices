import os

folder = os.path.dirname(__file__)
p = os.path.join(folder, 'data', 'test.csv')
with open(p, 'a+') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['test'])