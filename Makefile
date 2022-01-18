build:
	mpicc tema3.c -o tema3 -lm

run:
	mpirun --oversubscribe -np 9 tema3 12 0

clean:
	rm tema3