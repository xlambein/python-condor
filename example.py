#!/usr/bin/python3
import condor
import sys


def cross_product(xs):
    if len(xs) == 1:
        for x in xs[0]:
            yield (x,)
    else:
        for x in xs[0]:
            for y in cross_product(xs[1:]):
                yield (x,) + y


# The different values to try for each parameter
I = [1, 2, 3]
J = ['foo', 'bar']
K = [0.1, 0.2, 0.4]


# Condor jobs launcher
if not condor.is_condor_job():
    with condor.SelfJob(
            output='outputs/output.$(Process)',
            error='errors/error.$(Process)',
            log='condor.log',
            debug=False
    ) as job:
        for args in cross_product((I, J, K)):
            job.queue(arguments=args)

# Condor job
else:
    i, j, k = sys.argv[1:]
    i = int(i)
    k = float(k)

    a = j * i
    b = k ** i

    print('{}, {}'.format(a, b))

