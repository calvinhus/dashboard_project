attributes = ['latestPrice', 'marketCap', 'change', 'changePercent',
              'open', 'close', 'week52High', 'week52Low', 'currency']

empty = []
for i in attributes:
    empty.append(i)
print('Done')
print(type(empty))
empty = tuple(empty)
print(empty)
