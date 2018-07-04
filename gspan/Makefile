CPP = g++
INCLUDE = -I/home/twlee/rdf3x-gspan/include

# DEBUG and RELEASE settings
#CPPFLAGS = -DDEBUG -g -ggdb -Wall -Wno-deprecated ${INCLUDE}
#CPPFLAGS = -g -ggdb -Wall -Wno-deprecated ${INCLUDE}
#CPPFLAGS = -DNDEBUG -O3 -fomit-frame-pointer -funroll-loops -fPIC \
	-mtune=pentium4 -Wall -Wno-deprecated ${INCLUDE}

CPPFLAGS = -g -ggdb -Wall -Wno-deprecated ${INCLUDE}

OBJS = graph.o util.o dfs.o # ismin.o gspan.o  main.o


all:	test

test:	${OBJS} test.cpp 

clean:
	rm -f *.o test

