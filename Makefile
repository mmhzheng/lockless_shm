CXX = g++
CXXFLAGS = -std=c++11 -O2 -Wall -pthread
LDFLAGS = -lrt
INCLUDES = -Iinclude
SRC = src/main.cpp
TARGET = lockless_shm_demo

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
