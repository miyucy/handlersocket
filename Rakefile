require "bundler/gem_tasks"
require "rake/clean"
require "rake/testtask"
require "rbconfig"

DLEXT = RbConfig::CONFIG["DLEXT"]

file "ext/handlersocket_ext.#{DLEXT}" => Dir.glob("ext/*{.rb,.c,.cc}") do
  Dir.chdir("ext") do
     ruby "extconf.rb"
     sh "make"
  end
end

file "lib/handlersocket_ext.#{DLEXT}" => "ext/handlersocket_ext.#{DLEXT}" do
  cp "ext/handlersocket_ext.#{DLEXT}", "lib/handlersocket_ext.#{DLEXT}"
end

Rake::TestTask.new do |t|
  t.libs << 'test'
  t.ruby_opts << '-rubygems'
  t.ruby_opts << '-rbundler/setup'
  t.test_files = FileList['test/**/*_test.rb']
  t.verbose = true
end
task test: "lib/handlersocket_ext.#{DLEXT}"

task default: :test

%W[
  ext/*.bundle
  lib/*.bundle
  ext/Makefile
  ext/*.o
  ext/*.log
].each do |file|
  CLOBBER.include file
end

