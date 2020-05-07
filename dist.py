import numpy as np

amount = 1000.
total = 0
cnt = 0
#s = (np.random.pareto(10., 10)) + 1 / 100


lower = 1
shape = 5
size = 1000
s = np.random.pareto(shape, size)

max = np.amax(s)
sum = sum(s)
print('max:',max)
print('sum:',sum)

p2total = 0
for x in sorted(s):
    cnt += 1
    
    p2 =  x / sum
    p2total += p2
    
    d = amount * p2
    total += d

    print("%4d %12.3f %22.3f %12.5f" % (cnt, x, d, p2))


print("TOT  %12s %22.3f %12.5f" % ('', total, p2total))

