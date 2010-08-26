require 'mkmf'

$CFLAGS += "-I/usr/include/handlersocket"
$libs += "-lhsclient"

have_library("stdc++")
create_makefile('handler_socket')
