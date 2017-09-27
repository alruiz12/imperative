#!/usr/bin/python
import sys
import pylab as pl

def foo():
	return 1


if __name__ == '__main__':
	x = foo()
	nclust = int(sys.argv[1])
	clusters = list(range(nclust))
	for i in range(nclust):
		clusters[i] = []
		with open("/home/alvaro/imperative/"+sys.argv[i+2]) as f:
			a = (f.readlines()[0]).split(')')
			for n in range(len(a)):
				b = a[n].replace("(", "")
				c = b.split(',')	
				clusters[i].append(c)

			
	print (float(clusters[0][0][0]))
	for y in range(nclust):
		for u in range(len(clusters[y])):
			pl.scatter(float(clusters[y][u][0]),float(clusters[y][u][0]), c='r', marker='+')
	pl.show()
    
    
