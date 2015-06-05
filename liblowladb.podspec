Pod::Spec.new do |s|
  s.name             = "liblowladb"
  s.version          = "0.0.2"
  s.summary          = "A synchronizing embedded client database based on MongoDB."
  s.homepage         = "https://github.com/lowla/liblowladb"
  s.license          = 'MIT'
  s.author           = { "Mark Dixon" => "mark_dixon@teamstudio.com" }
  s.source           = { :git => "https://github.com/lowla/liblowladb.git", :tag => s.version.to_s }

  s.ios.deployment_target = '6.0'
  s.osx.deployment_target = '10.8'

  s.requires_arc = true
  s.compiler_flags = '-DMONGO_USE_LONG_LONG_INT'
  s.prefix_header_contents = '#ifndef NDEBUG','#define SQLITE_DEBUG 1','#endif'

  s.ios.source_files = 'src/**/*.*','platform-src/ios/**/*.*'
  s.osx.source_files = 'src/**/*.*','platform-src/osx/**/*.*'
end
