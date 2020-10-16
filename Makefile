
CC = g++
INCLUDE = -I.
FLAGS = -std=c++11 -lpthread

apps : test/test

test/% : test/%.cpp
	@mkdir -p bin/$(@D)
	$(CC) $@.cpp -o bin/$@ $(INCLUDE) $(FLAGS)

clean :
	-rm -rf bin