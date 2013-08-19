require 'mkmf'

dir_config 'handlersocket'

$CPPFLAGS = '-x c++ ' << $CPPFLAGS # as C++

found = true
unless have_header 'hstcpcli.hpp'
  %w[
    /usr/include/handlersocket
    /usr/local/include/handlersocket
  ].each do |dir|
    inc = " -I#{dir}"
    $CPPFLAGS << inc
    if have_header 'hstcpcli.hpp'
      found = true
      break
    else
      $CPPFLAGS = $CPPFLAGS.sub inc, ''
      found = false
    end
  end
end

unless found
  message = <<EOF
*************************************************************
Makefile creation failed

See https://github.com/miyucy/handlersocket/issues/1

$ apt-get install handlersocket-mysql-5.5 libhsclient-dev # in Ubuntu

Or, specify handlersocket header path with --with-opt-include=...

*************************************************************
EOF
  Logging.message message
  $stderr.puts message, 'Check mkmf.log'
  exit 1
end

$CPPFLAGS = $CPPFLAGS.sub '-x c++ ', ''

have_library 'hsclient'
have_library 'stdc++'
create_makefile 'handler_socket'
