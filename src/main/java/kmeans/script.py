#!/usr/bin/python
import sys
import pylab as pl
from random import randint

if __name__ == '__main__':
	# obaining args
	nclust = int(sys.argv[1])
	clusters = list(range(nclust))

	# setting colors and markers strings
	markers=['.',',','o','v','^','<','>','8','s','p','P','*','H','+',"x",'X','D','d','|','_']
	colors=['b','g','r','c','m','y','k','y']
	indexUsed={}
	point=[]
	
	# loading parsed args into memory
	for i in range(nclust):
		clusters[i] = []
		with open("/home/alvaro/imperative/"+str(i)) as f:
			a = (f.readlines()[0]).split(')')
			for n in range(len(a)):
				b = a[n].replace("(", "")
				c = b.split(',')	
				clusters[i].append(c)
	
	# print each cluster
	for y in range(nclust):			
		rand1=randint(0,len(colors)-1)
		rand2=randint(0,len(markers)-1)
		if len(indexUsed)>0:
			# if combination color-marker exists, don't repeat
			while (str(rand1)+'-'+str(rand2)) in indexUsed:
				rand1=randint(0,len(colors)-1)
				rand2=randint(0,len(markers)-1)
		point=str(rand1)+'-'+str(rand2)
		indexUsed.update({point: point})
		for u in range(len(clusters[y])):
			if len(clusters[y][u]) != 1:
				pl.scatter(clusters[y][u][0],clusters[y][u][1], c=colors[rand1], marker=markers[rand2])
	pl.show()

    
    
