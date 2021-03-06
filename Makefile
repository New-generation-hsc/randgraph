
CC = g++
INCLUDE = -I.
FLAGS = -std=c++11 -lpthread -fopenmp -Wall

apps : test/preprocess test/walk

test/% : test/%.cpp
	@mkdir -p bin/$(@D)
	$(CC) $@.cpp -o bin/$@ $(INCLUDE) $(FLAGS)

clean :
	-rm -rf bin

clear : 
	-rm dataset/*.deg dataset/*.csr dataset/*.beg dataset/*.rat dataset/*.blocks dataset/*.meta

walks:
	-rm dataset/*.walk