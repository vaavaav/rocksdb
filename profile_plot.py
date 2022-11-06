#!/usr/bin/env python3

import json
from pandas import DataFrame
from matplotlib import pyplot

profile = json.load(open('trace_file.json'))

total = profile["total"]
inserts = DataFrame([{y:x[y]["I"] for y in x.keys()} for x in profile["time-series"]])
look_ups = DataFrame([{y:x[y]["L"] for y in x.keys()} for x in profile["time-series"]])

inserts.plot()
pyplot.title('Inserts per second')
pyplot.xlabel('seconds')
pyplot.ylabel('#inserts')
pyplot.savefig('profile_inserts.png')

look_ups.plot()
pyplot.title('LookUps per second')
pyplot.xlabel('seconds')
pyplot.ylabel('#lookups')
pyplot.savefig('profile_lookups.png')