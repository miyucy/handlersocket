require "mkmf"

$CFLAGS << " -I/usr/include/handlersocket"
have_library("hsclient")
have_library("stdc++")
create_makefile("handler_socket")
