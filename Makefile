.SUFFIXS: .cpp .o

CXX=g++

SRCDIR=src/
UNIT_DIR=unittest/
INC=include/
LIBS=lib/

# lock table test file
LT_SRC:=$(UNIT_DIR)unittest_lock_table.cpp
LT_OBJ:=$(LT_SRC:.cpp=.o)
LT_TARGET=unittest_lock_table

# DON'T TOUCH THIS !!!!!!!
MARKER_SRC:=$(UNIT_DIR)unittest_marker.cpp
MARKER_OBJ:=$(MARKER_SRC:.cpp=.o)
MARKER_TARGET=unittest_marker
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRC:= \
	$(SRCDIR)lock_manager.cpp \
	$(SRCDIR)transaction_manager.cpp \
	$(SRCDIR)buf.cpp \
	$(SRCDIR)bpt.cpp \
	$(SRCDIR)file.cpp \

OBJ:=$(SRC:.cpp=.o)

CXXFLAGS+= -g -std=c++17 -Wall -fPIC -I $(INC)

unit_lt: $(LT_TARGET)

$(LT_TARGET): $(LT_OBJ) $(OBJ) $(STATIC_LIB)
	$(CXX) $(CXXFLAGS) $< -o $@ $(OBJ) -lpthread

unit_marker: $(MARKER_TARGET)

$(MARKER_TARGET): $(MARKER_OBJ) $(OBJ)
	$(CXX) $(CXXFLAGS) $< -o $@ $(OBJ) -lpthread

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $^ -c -o $@ -lpthread

clean:
	$(RM) $(OBJ)
	$(RM) $(TARGET) $(TARGET_OBJ)
	$(RM) $(LT_TARGET) $(LT_OBJ)
	$(RM) $(MARKER_TARGET) $(MARKER_OBJ)
	$(RM) $(LIBS)*

$(STATIC_LIB): $(OBJ)
	ar cr $@ $^
