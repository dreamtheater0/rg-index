find . \( -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp'  \) -print > cscope.files
cscope -i cscope.files

