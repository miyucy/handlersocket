require "mkmf"

inc_dirs = dir_config("handlersocket", '/usr/local/include', '/usr/include')

# $CFLAGS << " -I/usr/include/handlersocket"
inc_dirs.each do |inc_dir|
  $CFLAGS << ' -I' + inc_dir + '/handlersocket'
end

puts "CFLAGS: #{$CFLAGS}"
have_library("hsclient")
have_library("stdc++")
create_makefile("handler_socket")
